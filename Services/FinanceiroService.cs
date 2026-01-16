using ExcelDataReader;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Globalization;

namespace MigrationProject.Services
{
    // Classe interna para representar a linha (Self-contained)
    public class ContaRow
    {
        public string IdVenda { get; set; }
        public string NumParcela { get; set; }
        public string CodClienteAntigo { get; set; }
        public DateTime? DataEmissao { get; set; }
        public DateTime? Vencimento { get; set; }
        public decimal Valor { get; set; }
        public decimal PagtoValor { get; set; }
        public DateTime? PagtoData { get; set; }
    }

    public class FinanceiroService
    {
        private string _connString;
        private Dictionary<string, int> _mapaClientes;
        private CultureInfo _cultureBR = new CultureInfo("pt-BR");

        public FinanceiroService(string connString)
        {
            _connString = connString;
            _mapaClientes = new Dictionary<string, int>();

            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
        }

        public void ExecutarMigracao(string arquivoClientes, string arquivoContas)
        {
            Console.WriteLine("=== INICIANDO MIGRAÇÃO FINANCEIRA (SEM ERRO ID_VENDA) ===");

            if (!CarregarMapaDeClientes(arquivoClientes)) return;
            ImportarContas(arquivoContas);
        }

        private bool CarregarMapaDeClientes(string path)
        {
            Console.WriteLine("1. Mapeando Clientes...");
            if (!File.Exists(path)) { Console.WriteLine($"[ERRO] Arquivo não encontrado: {path}"); return false; }

            var mapAntigoParaCpf = new Dictionary<string, string>();

            try
            {
                using (var stream = File.Open(path, FileMode.Open, FileAccess.Read))
                using (var reader = ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration() { AutodetectSeparators = new char[] { ';', ',', '\t' }, FallbackEncoding = System.Text.Encoding.GetEncoding(1252) }))
                {
                    var result = reader.AsDataSet(new ExcelDataSetConfiguration() { ConfigureDataTable = (_) => new ExcelDataTableConfiguration() { UseHeaderRow = true } });
                    foreach (DataRow row in result.Tables[0].Rows)
                    {
                        string id = LerString(row, 0);
                        string cpf = LerString(row, 2);

                        if (!string.IsNullOrEmpty(cpf)) cpf = Regex.Replace(cpf, "[^0-9]", "");

                        if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(cpf) && !mapAntigoParaCpf.ContainsKey(id))
                            mapAntigoParaCpf[id] = cpf;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro crítico lendo clientes: {ex.Message}");
                return false;
            }

            var mapCpfParaNovo = new Dictionary<string, int>();
            using (var conn = new NpgsqlConnection(_connString))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand("SELECT cpf_cnpj, id FROM public.clientes WHERE cpf_cnpj IS NOT NULL", conn))
                using (var r = cmd.ExecuteReader())
                {
                    while (r.Read())
                    {
                        string cpf = Regex.Replace(r.GetString(0), "[^0-9]", "");
                        if (!string.IsNullOrEmpty(cpf) && !mapCpfParaNovo.ContainsKey(cpf))
                            mapCpfParaNovo[cpf] = r.GetInt32(1);
                    }
                }
            }

            foreach (var item in mapAntigoParaCpf)
            {
                if (mapCpfParaNovo.ContainsKey(item.Value))
                    _mapaClientes[item.Key] = mapCpfParaNovo[item.Value];
            }
            Console.WriteLine($"-> Vínculos confirmados: {_mapaClientes.Count}\n");
            return true;
        }

        private void ImportarContas(string filePath)
        {
            if (!File.Exists(filePath)) { Console.WriteLine($"[ERRO] Arquivo não encontrado: {filePath}"); return; }

            Console.WriteLine("2. Lendo Arquivo de Contas...");
            var listaLinhas = new List<ContaRow>();

            try
            {
                using (var stream = File.Open(filePath, FileMode.Open, FileAccess.Read))
                using (var reader = ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration() { AutodetectSeparators = new char[] { ';', ',', '\t' }, FallbackEncoding = System.Text.Encoding.GetEncoding(1252) }))
                {
                    var result = reader.AsDataSet(new ExcelDataSetConfiguration() { ConfigureDataTable = (_) => new ExcelDataTableConfiguration() { UseHeaderRow = true } });

                    var cols = result.Tables[0].Columns;

                    // Mapeamento de índices das colunas
                    int idxIdVenda = cols.Contains("IdNumero_Dup") ? cols["IdNumero_Dup"].Ordinal : 7;
                    int idxIdPessoa = cols.Contains("IdPessoa_Dup") ? cols["IdPessoa_Dup"].Ordinal : 13;
                    int idxEmissao = cols.Contains("DataEmissao_Dup") ? cols["DataEmissao_Dup"].Ordinal : 18;
                    int idxVencimento = cols.Contains("Vencimento_Dup") ? cols["Vencimento_Dup"].Ordinal : 19;
                    int idxValor = cols.Contains("Valor_Dup") ? cols["Valor_Dup"].Ordinal : 22;
                    int idxPagtoVal = cols.Contains("PagtoValor_Dup") ? cols["PagtoValor_Dup"].Ordinal : 27;
                    int idxPagtoData = cols.Contains("PagtoData_Dup") ? cols["PagtoData_Dup"].Ordinal : 30;
                    int idxParcela = cols.Contains("IdParcela_Dup") ? cols["IdParcela_Dup"].Ordinal : 2;

                    foreach (DataRow row in result.Tables[0].Rows)
                    {
                        string idVenda = LerString(row, idxIdVenda);
                        if (string.IsNullOrWhiteSpace(idVenda)) continue;

                        listaLinhas.Add(new ContaRow
                        {
                            IdVenda = idVenda,
                            NumParcela = LerString(row, idxParcela),
                            CodClienteAntigo = LerString(row, idxIdPessoa),
                            DataEmissao = LerData(row, idxEmissao),
                            Vencimento = LerData(row, idxVencimento),
                            Valor = LerDecimal(row, idxValor),
                            PagtoValor = LerDecimal(row, idxPagtoVal),
                            PagtoData = LerData(row, idxPagtoData)
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERRO AO LER CSV CONTAS: {ex.Message}");
                return;
            }

            if (listaLinhas.Count == 0) { Console.WriteLine("Nenhum dado lido do CSV."); return; }

            var grupos = listaLinhas.GroupBy(x => x.IdVenda).ToList();
            SalvarEmLotes(grupos);
        }

        private void SalvarEmLotes(List<IGrouping<string, ContaRow>> grupos)
        {
            Console.WriteLine("3. Salvando no Banco...");
            int sucesso = 0;
            int ignorados = 0;
            int erros = 0;
            int total = grupos.Count;
            int processados = 0;

            using (var conn = new NpgsqlConnection(_connString))
            {
                conn.Open();
                int batchSize = 50;

                for (int i = 0; i < total; i += batchSize)
                {
                    var lote = grupos.Skip(i).Take(batchSize);

                    using (var transaction = conn.BeginTransaction())
                    {
                        try
                        {
                            foreach (var venda in lote)
                            {
                                processados++;
                                var dados = venda.First();

                                if (!_mapaClientes.TryGetValue(dados.CodClienteAntigo, out int idClienteNovo))
                                {
                                    ignorados++;
                                    continue;
                                }

                                // 1. Grava ID Original na OBSERVAÇÃO (para não dar erro de coluna id_venda)
                                string obs = $"Migração Venda Original: {dados.IdVenda}";

                                string sqlCapa = @"
                                    INSERT INTO public.contas_receber (
                                        id_empresa, id_cliente, data_emissao, valor_total, 
                                        num_parcelas, tipo_cobranca, data_cadastro, 
                                        id_plano_conta, id_usuario_cadastro, observacao
                                    ) VALUES (
                                        1, @cli, @emi, @val, 
                                        @parc, 1, CURRENT_TIMESTAMP, 
                                        3, 1, @obs
                                    ) RETURNING id;";

                                int idConta = 0;
                                using (var cmd = new NpgsqlCommand(sqlCapa, conn, transaction))
                                {
                                    cmd.Parameters.AddWithValue("cli", idClienteNovo);
                                    cmd.Parameters.AddWithValue("emi", dados.DataEmissao ?? DateTime.Now);
                                    cmd.Parameters.AddWithValue("val", venda.Sum(x => x.Valor));
                                    cmd.Parameters.AddWithValue("parc", venda.Count());
                                    cmd.Parameters.AddWithValue("obs", obs);

                                    idConta = (int)cmd.ExecuteScalar();
                                }

                                // 2. Grava Parcelas
                                foreach (var p in venda)
                                {
                                    int situacao = (p.PagtoValor > 0 || p.PagtoData.HasValue) ? 1 : 0;
                                    int.TryParse(p.NumParcela, out int np); if (np == 0) np = 1;

                                    string sqlItem = @"
                                        INSERT INTO public.contas_receber_parcelas (
                                            id_conta_receber, num_parcela, sequencia, data_vencimento, 
                                            valor_parcela, valor_pago, data_pagamento, situacao
                                        ) VALUES (
                                            @idC, @np, @np, @venc, 
                                            @val, @vpago, @dpago, @sit
                                        );";

                                    using (var cmd = new NpgsqlCommand(sqlItem, conn, transaction))
                                    {
                                        cmd.Parameters.AddWithValue("idC", idConta);
                                        cmd.Parameters.AddWithValue("np", np);
                                        cmd.Parameters.AddWithValue("venc", p.Vencimento ?? DateTime.Now);
                                        cmd.Parameters.AddWithValue("val", p.Valor);
                                        cmd.Parameters.AddWithValue("vpago", p.PagtoValor);
                                        cmd.Parameters.AddWithValue("dpago", p.PagtoData ?? (object)DBNull.Value);
                                        cmd.Parameters.AddWithValue("sit", situacao);
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                                sucesso++;
                            }
                            transaction.Commit();
                            Console.Write($"\rProgresso: {processados}/{total} | Sucesso: {sucesso}");
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            erros += batchSize;
                            Console.WriteLine($"\n[ERRO LOTE]: {ex.Message}");
                        }
                    }
                }
            }

            // Atualiza Sequências (Ignorando erros caso a sequência não exista)
            try
            {
                using (var conn = new NpgsqlConnection(_connString))
                {
                    conn.Open();
                    new NpgsqlCommand("SELECT setval('public.contas_receber_id_seq', (SELECT COALESCE(MAX(id), 1) FROM public.contas_receber));", conn).ExecuteNonQuery();
                    new NpgsqlCommand("SELECT setval('public.contas_receber_parcelas_id_seq', (SELECT COALESCE(MAX(id), 1) FROM public.contas_receber_parcelas));", conn).ExecuteNonQuery();
                }
            }
            catch { }

            Console.WriteLine($"\n\n[FINALIZADO] Sucesso: {sucesso} | Ignorados: {ignorados} | Erros: {erros}");
        }

        //Metodos Locais
        private string LerString(DataRow row, int index)
        {
            if (row == null || index < 0 || index >= row.ItemArray.Length) return null;
            object val = row[index];
            if (val == null || val == DBNull.Value) return null;
            return val.ToString().Trim();
        }

        private decimal LerDecimal(DataRow row, int index)
        {
            string val = LerString(row, index);
            if (string.IsNullOrEmpty(val)) return 0;
            val = val.Replace("R$", "").Trim();
            if (decimal.TryParse(val, NumberStyles.Any, _cultureBR, out decimal result)) return result;
            return 0;
        }

        private DateTime? LerData(DataRow row, int index)
        {
            string val = LerString(row, index);
            if (string.IsNullOrEmpty(val) || val.ToUpper() == "NULL") return null;
            if (DateTime.TryParse(val, out DateTime dt)) return dt;
            return null;
        }
    }
}