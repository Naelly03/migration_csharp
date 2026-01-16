using ExcelDataReader;
using Npgsql;
using System;
using System.IO;
using System.Data;
using System.Text.RegularExpressions;

namespace MigrationProject.Services
{
    public class ClienteService
    {
        private string _connString;
        public ClienteService(string connString) { _connString = connString; }

        public void ImportarClientes(string filePath)
        {
            if (!File.Exists(filePath)) { Console.WriteLine($"[ERRO] Arquivo não encontrado: {filePath}"); return; }
            Console.WriteLine("--- IMPORTANDO CLIENTES ---");

            using (var stream = File.Open(filePath, FileMode.Open, FileAccess.Read))
            using (var reader = ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration() { AutodetectSeparators = new char[] { ';', ',' }, FallbackEncoding = System.Text.Encoding.GetEncoding(1252) }))
            using (var conn = new NpgsqlConnection(_connString))
            {
                var result = reader.AsDataSet(new ExcelDataSetConfiguration() { ConfigureDataTable = (_) => new ExcelDataTableConfiguration() { UseHeaderRow = true } });
                conn.Open();

                int sucesso = 0;
                int erros = 0;
                int ignorados = 0;

                foreach (DataRow row in result.Tables[0].Rows)
                {
                    try
                    {
                        string cpfCnpj = Utils.GetVal(row, 2);
                        string nome = Utils.GetVal(row, 29);
                        string fantasia = Utils.GetVal(row, 30);
                        string endereco = Utils.GetVal(row, 36);
                        string numeroStr = Utils.GetVal(row, 37);

                        if (string.IsNullOrWhiteSpace(nome))
                        {
                            ignorados++;
                            continue;
                        }

                        if (!string.IsNullOrEmpty(cpfCnpj))
                            cpfCnpj = Regex.Replace(cpfCnpj, "[^0-9]", ""); 

                        if (string.IsNullOrEmpty(cpfCnpj)) cpfCnpj = null; 

                        int? numero = null;
                        if (!string.IsNullOrEmpty(numeroStr))
                        {
                            string apenasDigitos = Regex.Replace(numeroStr, "[^0-9]", "");
                            if (int.TryParse(apenasDigitos, out int n)) numero = n;
                        }

                        
                        string sql = @"
                            INSERT INTO public.clientes (
                                razao_social, nome_fantasia, cpf_cnpj, logradouro, numero, 
                                data_cadastro, situacao
                            )
                            VALUES (
                                @razao, @fantasia, @cpf, @end, @num, 
                                CURRENT_TIMESTAMP, 1
                            )
                            ON CONFLICT (cpf_cnpj) DO NOTHING;"; 

                        using (var cmd = new NpgsqlCommand(sql, conn))
                        {
                            cmd.Parameters.AddWithValue("razao", nome);
                            cmd.Parameters.AddWithValue("fantasia", fantasia ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("cpf", cpfCnpj ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("end", endereco ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("num", numero ?? (object)DBNull.Value);

                            int rows = cmd.ExecuteNonQuery();
                            if (rows > 0) sucesso++;
                            else ignorados++; 
                        }
                    }
                    catch (Exception ex)
                    {
                        erros++;
                        if (erros <= 5) Console.WriteLine($"[ERRO SQL Cliente]: {ex.Message}");
                    }
                }
                Console.WriteLine($"\n[RESUMO CLIENTES]");
                Console.WriteLine($"Importados: {sucesso}");
                Console.WriteLine($"Ignorados (Já existiam/Vazios): {ignorados}");
                Console.WriteLine($"Erros: {erros}");
            }
        }
    }
}