using ExcelDataReader;
using Npgsql;
using System;
using System.IO;
using System.Data;
using System.Text.RegularExpressions;

namespace MigrationProject.Services
{
    public class FornecedorService
    {
        private string _connString;
        public FornecedorService(string connString) { _connString = connString; }

        public void ImportarFornecedores(string filePath)
        {
            if (!File.Exists(filePath)) { Console.WriteLine($"[ERRO] Arquivo não encontrado: {filePath}"); return; }
            Console.WriteLine("--- IMPORTANDO FORNECEDORES ---");

            using (var stream = File.Open(filePath, FileMode.Open, FileAccess.Read))
            using (var reader = ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration() { AutodetectSeparators = new char[] { ';', ',' }, FallbackEncoding = System.Text.Encoding.GetEncoding(1252) }))
            using (var conn = new NpgsqlConnection(_connString))
            {
                var result = reader.AsDataSet(new ExcelDataSetConfiguration() { ConfigureDataTable = (_) => new ExcelDataTableConfiguration() { UseHeaderRow = false } });
                conn.Open();

                int novos = 0;
                int erros = 0;
                int total = result.Tables[0].Rows.Count;
                int processados = 0;

                foreach (DataRow row in result.Tables[0].Rows)
                {
                    processados++;
                    if (processados % 100 == 0) Console.Write($"\rProcessando: {processados}/{total} | Novos: {novos}");

                    try
                    {
                        string idAntigo = Utils.GetVal(row, 0);
                        string cnpj = Utils.GetVal(row, 2);
                        string razao = Utils.GetVal(row, 29);
                        string fantasia = Utils.GetVal(row, 30);
                        string endereco = Utils.GetVal(row, 36);
                        string numeroStr = Utils.GetVal(row, 37);
                        string cep = Utils.GetVal(row, 40);

                        if (string.IsNullOrEmpty(razao)) continue;
                        if (!int.TryParse(idAntigo, out int id)) continue;

                        // Limpeza
                        if (!string.IsNullOrEmpty(cnpj))
                        {
                            cnpj = Regex.Replace(cnpj, "[^0-9]", "");
                            if (cnpj.Length > 18) cnpj = cnpj.Substring(0, 18);
                        }
                        if (string.IsNullOrEmpty(cnpj)) cnpj = null;

                        int? numero = null;
                        if (!string.IsNullOrEmpty(numeroStr))
                        {
                            string digitos = Regex.Replace(numeroStr, "[^0-9]", "");
                            if (int.TryParse(digitos, out int n)) numero = n;
                        }

                        string sql = @"
                            INSERT INTO public.fornecedores (
                                id, razao_social, nome_fantasia, cpf_cnpj, 
                                logradouro, numero, cep, 
                                data_cadastro
                            )
                            VALUES (
                                @id, @razao, @fantasia, @cnpj, 
                                @end, @num, @cep, 
                                CURRENT_TIMESTAMP
                            )
                            ON CONFLICT (id) DO NOTHING;";

                        using (var cmd = new NpgsqlCommand(sql, conn))
                        {
                            cmd.Parameters.AddWithValue("id", id);
                            cmd.Parameters.AddWithValue("razao", razao);
                            cmd.Parameters.AddWithValue("fantasia", fantasia ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("cnpj", cnpj ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("end", endereco ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("num", numero ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("cep", cep ?? (object)DBNull.Value);

                            if (cmd.ExecuteNonQuery() > 0) novos++;
                        }
                    }
                    catch (Exception ex)
                    {
                        erros++;
                        if (erros <= 3) Console.WriteLine($"\n[ERRO Fornecedor]: {ex.Message}");
                    }
                }

                Console.WriteLine("\n[RESUMO FORNECEDORES]");
                Console.WriteLine($"Importados: {novos} | Erros: {erros}");
                try { using (var cmd = new NpgsqlCommand("SELECT setval('public.fornecedores_id_seq', (SELECT MAX(id) FROM public.fornecedores));", conn)) cmd.ExecuteNonQuery(); } catch { }
            }
        }
    }
}