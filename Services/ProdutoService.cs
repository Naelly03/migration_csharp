using ExcelDataReader;
using Npgsql;
using System;
using System.IO;
using System.Data;

namespace MigrationProject.Services
{
    public class ProdutoService
    {
        private string _connString;
        public ProdutoService(string connString) { _connString = connString; }

        public void ImportarProdutos(string filePath)
        {
            if (!File.Exists(filePath)) { Console.WriteLine($"[ERRO] Arquivo não encontrado: {filePath}"); return; }
            Console.WriteLine("--- CADASTRANDO PRODUTOS ---");

            using (var stream = File.Open(filePath, FileMode.Open, FileAccess.Read))
            using (var reader = ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration() { AutodetectSeparators = new char[] { ';', ',' }, FallbackEncoding = System.Text.Encoding.GetEncoding(1252) }))
            using (var conn = new NpgsqlConnection(_connString))
            {
                var result = reader.AsDataSet(new ExcelDataSetConfiguration() { ConfigureDataTable = (_) => new ExcelDataTableConfiguration() { UseHeaderRow = true } });
                conn.Open();

                int novos = 0;
                int jaExistiam = 0;
                int erros = 0;
                int processados = 0;
                int total = result.Tables[0].Rows.Count;

                Console.WriteLine($"Total de produtos: {total}");

                foreach (DataRow row in result.Tables[0].Rows)
                {
                    processados++;
                    if (processados % 500 == 0) Console.Write($"\rProcessando: {processados}/{total} | Novos: {novos}");

                    try
                    {
                        string idAntigo = Utils.GetVal(row, 0);
                        string nome = Utils.GetVal(row, 3);
                        string unidade = Utils.GetVal(row, 5);
                        string ncm = Utils.GetVal(row, 15);

                        if (!int.TryParse(idAntigo, out int id)) continue;
                        if (string.IsNullOrEmpty(nome)) continue;

                        // Tratamento Unidade
                        string unTratada = string.IsNullOrWhiteSpace(unidade) ? "UN" : unidade.Trim();
                        if (unTratada.Length > 6) unTratada = unTratada.Substring(0, 6);

                        string sql = @"
                            INSERT INTO public.produtos (
                                id, descricao, 
                                id_unidade, id_unidade_armaz, 
                                id_ncm, data_cadastro, ativo
                            )
                            VALUES (
                                @id, @nome, 
                                @un, @un, 
                                @ncm, CURRENT_TIMESTAMP, true
                            )
                            ON CONFLICT (id) DO NOTHING;";

                        using (var cmd = new NpgsqlCommand(sql, conn))
                        {
                            cmd.Parameters.AddWithValue("id", id);
                            cmd.Parameters.AddWithValue("nome", nome);
                            cmd.Parameters.AddWithValue("un", unTratada);

                            // Tenta converter NCM para inteiro se o banco exigir, senão manda string ou null
                            if (int.TryParse(ncm, out int ncmInt))
                                cmd.Parameters.AddWithValue("ncm", ncmInt);
                            else
                                cmd.Parameters.AddWithValue("ncm", DBNull.Value);

                            int linhas = cmd.ExecuteNonQuery();
                            if (linhas > 0) novos++; else jaExistiam++;
                        }
                    }
                    catch (Exception ex)
                    {
                        erros++;
                        if (erros <= 3) Console.WriteLine($"\n[ERRO Prod ID {Utils.GetVal(row, 0)}]: {ex.Message}");
                    }
                }

                Console.WriteLine("\n[RESUMO PRODUTOS]");
                Console.WriteLine($"Novos: {novos} | Existentes: {jaExistiam} | Erros: {erros}");

                // Atualiza sequence
                try { using (var cmd = new NpgsqlCommand("SELECT setval('public.produtos_id_seq', (SELECT MAX(id) FROM public.produtos));", conn)) cmd.ExecuteNonQuery(); } catch { }
            }
        }

        public void AtualizarPrecos(string filePath)
        {
            if (!File.Exists(filePath)) { Console.WriteLine($"[ERRO] Arquivo não encontrado: {filePath}"); return; }
            Console.WriteLine("--- ATUALIZANDO PREÇOS ---");

            using (var stream = File.Open(filePath, FileMode.Open, FileAccess.Read))
            using (var reader = ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration() { AutodetectSeparators = new char[] { ';', ',' }, FallbackEncoding = System.Text.Encoding.GetEncoding(1252) }))
            using (var conn = new NpgsqlConnection(_connString))
            {
                var result = reader.AsDataSet(new ExcelDataSetConfiguration() { ConfigureDataTable = (_) => new ExcelDataTableConfiguration() { UseHeaderRow = true } });
                conn.Open();

                int atualizados = 0;

                foreach (DataRow row in result.Tables[0].Rows)
                {
                    // Tenta buscar por nome da coluna, se falhar tenta índice
                    string idStr = Utils.GetVal(row, "Id_PRO") ?? Utils.GetVal(row, 0);
                    decimal venda = Utils.GetDecimal(row, "PrecoVenda_PRV");
                    decimal compra = Utils.GetDecimal(row, "PrecoCompra");

                    if (!int.TryParse(idStr, out int id)) continue;

                    string sql = "UPDATE public.produtos SET valor_compra = @compra, valor_venda = @venda WHERE id = @id";
                    using (var cmd = new NpgsqlCommand(sql, conn))
                    {
                        cmd.Parameters.AddWithValue("compra", compra);
                        cmd.Parameters.AddWithValue("venda", venda);
                        cmd.Parameters.AddWithValue("id", id);
                        if (cmd.ExecuteNonQuery() > 0) atualizados++;
                    }
                }
                Console.WriteLine($"[SUCESSO] Preços atualizados: {atualizados}");
            }
        }
    }
}