using ExcelDataReader;
using Npgsql;
using System;
using System.IO;
using System.Data;
using System.Collections.Generic;

namespace MigrationProject.Services
{
    public class ProdutoUpdateService
    {
        private string _connString;

        public ProdutoUpdateService(string connString)
        {
            _connString = connString;
        }

        public void AtualizarCodigoBarrasEReferencia(string filePath)
        {
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"[ERRO] Arquivo não encontrado: {filePath}");
                return;
            }

            Console.WriteLine("=== ATUALIZANDO CÓDIGO DE BARRAS E REFERÊNCIA ===");
            Console.WriteLine($"Arquivo: {Path.GetFileName(filePath)}");
            Console.WriteLine("ATENÇÃO: Só atualiza se o campo estiver VAZIO no banco\n");

            List<string> errosDetalhados = new List<string>();
            List<string> avisosDetalhados = new List<string>();
            int maxErrosExibir = 20;

            try
            {
                using (var stream = File.Open(filePath, FileMode.Open, FileAccess.Read))
                using (var reader = ExcelReaderFactory.CreateCsvReader(stream,
                    new ExcelReaderConfiguration()
                    {
                        AutodetectSeparators = new char[] { '\t', ';', ',' },
                        FallbackEncoding = System.Text.Encoding.GetEncoding(1252)
                    }))
                using (var conn = new NpgsqlConnection(_connString))
                {
                    var result = reader.AsDataSet(new ExcelDataSetConfiguration()
                    {
                        ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                        {
                            UseHeaderRow = false
                        }
                    });

                    DataTable table = result.Tables[0];

                    Console.WriteLine($"Total de colunas: {table.Columns.Count}");
                    Console.WriteLine($"Total de linhas: {table.Rows.Count}");

                    conn.Open();

                    int atualizados = 0;
                    int erros = 0;
                    int naoEncontrados = 0;
                    int jaPreenchidos = 0; 
                    int duplicados = 0;
                    int processados = 0;
                    int linhasIgnoradas = 0;
                    int total = table.Rows.Count;

                    Console.WriteLine($"\nTotal de registros para processar: {total}");

                    foreach (DataRow row in table.Rows)
                    {
                        processados++;

                        if (processados % 100 == 0)
                            Console.Write($"\rProcessando: {processados}/{total} | Atualizados: {atualizados} | Já tinham: {jaPreenchidos} | Erros: {erros}");

                        string idProdutoStr = Utils.GetVal(row, 0);
                        string nomeProduto = Utils.GetVal(row, 1);
                        string ean = Utils.GetVal(row, 2);
                        string referenciaFabrica = Utils.GetVal(row, 3);

                        if (string.IsNullOrWhiteSpace(idProdutoStr) &&
                            string.IsNullOrWhiteSpace(nomeProduto) &&
                            string.IsNullOrWhiteSpace(ean) &&
                            string.IsNullOrWhiteSpace(referenciaFabrica))
                        {
                            linhasIgnoradas++;
                            continue;
                        }

                        // Validação do ID
                        if (string.IsNullOrWhiteSpace(idProdutoStr))
                        {
                            errosDetalhados.Add($"Linha {processados}: ID vazio - Nome: '{nomeProduto}'");
                            erros++;
                            continue;
                        }

                        if (!int.TryParse(idProdutoStr, out int idProduto))
                        {
                            errosDetalhados.Add($"Linha {processados}: ID inválido '{idProdutoStr}' - Nome: '{nomeProduto}'");
                            erros++;
                            continue;
                        }

                        // Tratamento dos valores
                        string codigoBarras = string.IsNullOrWhiteSpace(ean) ||
                                              ean.ToUpper() == "NULL" ||
                                              ean.ToUpper() == "\"NULL\""
                            ? null
                            : ean.Trim();

                        string referencia = string.IsNullOrWhiteSpace(referenciaFabrica) ||
                                           referenciaFabrica.ToUpper() == "NULL" ||
                                           referenciaFabrica.ToUpper() == "\"NULL\""
                            ? null
                            : referenciaFabrica.Trim();

                        // Se ambos estiverem vazios, pula
                        if (string.IsNullOrWhiteSpace(codigoBarras) && string.IsNullOrWhiteSpace(referencia))
                        {
                            continue;
                        }

                        try
                        {
                            // Verifica se o produto existe e quais campos já estão preenchidos
                            bool produtoExiste = false;
                            bool codBarrasPreenchido = false;
                            bool referenciaPreenchida = false;

                            string verificaSql = "SELECT cod_barras, referencia FROM public.produtos WHERE id = @id";
                            using (var cmdVerifica = new NpgsqlCommand(verificaSql, conn))
                            {
                                cmdVerifica.Parameters.AddWithValue("@id", idProduto);
                                using (var readerDb = cmdVerifica.ExecuteReader())
                                {
                                    if (readerDb.Read())
                                    {
                                        produtoExiste = true;
                                        var codBarrasDb = readerDb.IsDBNull(0) ? null : readerDb.GetString(0);
                                        var referenciaDb = readerDb.IsDBNull(1) ? null : readerDb.GetString(1);

                                        codBarrasPreenchido = !string.IsNullOrWhiteSpace(codBarrasDb);
                                        referenciaPreenchida = !string.IsNullOrWhiteSpace(referenciaDb);
                                    }
                                    readerDb.Close();
                                }
                            }

                            if (!produtoExiste)
                            {
                                naoEncontrados++;
                                avisosDetalhados.Add($"Produto não encontrado - ID: {idProduto}, Nome: {nomeProduto}");
                                continue;
                            }

                            // Verifica o que precisa ser atualizado
                            bool atualizarCodBarras = !string.IsNullOrWhiteSpace(codigoBarras) && !codBarrasPreenchido;
                            bool atualizarReferencia = !string.IsNullOrWhiteSpace(referencia) && !referenciaPreenchida;

                            // Se algum campo já estiver preenchido, registra como aviso
                            if (!string.IsNullOrWhiteSpace(codigoBarras) && codBarrasPreenchido)
                            {
                                jaPreenchidos++;
                                avisosDetalhados.Add($"Código de barras já preenchido (mantido) - ID: {idProduto}");
                                atualizarCodBarras = false;
                            }

                            if (!string.IsNullOrWhiteSpace(referencia) && referenciaPreenchida)
                            {
                                jaPreenchidos++;
                                avisosDetalhados.Add($"Referência já preenchida (mantida) - ID: {idProduto}");
                                atualizarReferencia = false;
                            }

                            // Se não tem nada para atualizar, pula
                            if (!atualizarCodBarras && !atualizarReferencia)
                            {
                                if (!string.IsNullOrWhiteSpace(codigoBarras) || !string.IsNullOrWhiteSpace(referencia))
                                {
                                    continue;
                                }
                            }

                            // Verifica duplicidade APENAS se for atualizar código de barras
                            if (atualizarCodBarras)
                            {
                                bool codigoBarrasDuplicado = false;
                                string verificaDuplicadoSql =
                                    "SELECT 1 FROM public.produtos WHERE cod_barras = @cod_barras AND id != @id";
                                using (var cmdVerificaDuplicado = new NpgsqlCommand(verificaDuplicadoSql, conn))
                                {
                                    cmdVerificaDuplicado.Parameters.AddWithValue("@cod_barras", codigoBarras);
                                    cmdVerificaDuplicado.Parameters.AddWithValue("@id", idProduto);
                                    codigoBarrasDuplicado = cmdVerificaDuplicado.ExecuteScalar() != null;
                                }

                                if (codigoBarrasDuplicado)
                                {
                                    duplicados++;
                                    avisosDetalhados.Add($"Código de barras duplicado (ignorado) - ID: {idProduto}, EAN: {codigoBarras}");
                                    atualizarCodBarras = false;
                                }
                            }

                            // Executa as atualizações necessárias
                            int linhasAfetadas = 0;

                            if (atualizarCodBarras)
                            {
                                string sqlCodBarras = "UPDATE public.produtos SET cod_barras = @cod_barras WHERE id = @id";
                                using (var cmd = new NpgsqlCommand(sqlCodBarras, conn))
                                {
                                    cmd.Parameters.AddWithValue("@id", idProduto);
                                    cmd.Parameters.AddWithValue("@cod_barras", codigoBarras);
                                    linhasAfetadas += cmd.ExecuteNonQuery();
                                }
                            }

                            if (atualizarReferencia)
                            {
                                string sqlReferencia = "UPDATE public.produtos SET referencia = @referencia WHERE id = @id";
                                using (var cmd = new NpgsqlCommand(sqlReferencia, conn))
                                {
                                    cmd.Parameters.AddWithValue("@id", idProduto);
                                    cmd.Parameters.AddWithValue("@referencia", referencia);
                                    linhasAfetadas += cmd.ExecuteNonQuery();
                                }
                            }

                            if (linhasAfetadas > 0)
                            {
                                atualizados++;
                            }
                        }
                        catch (NpgsqlException npgEx)
                        {
                            erros++;
                            string erroMsg = $"Linha {processados}, ID {idProduto}: PostgreSQL Error {npgEx.SqlState} - {npgEx.Message}";
                            errosDetalhados.Add(erroMsg);
                        }
                        catch (Exception ex)
                        {
                            erros++;
                            string erroMsg = $"Linha {processados}, ID {idProduto}: {ex.Message}";
                            errosDetalhados.Add(erroMsg);
                        }
                    }

                    Console.WriteLine($"\n\n[RESUMO]");
                    Console.WriteLine($"Total de linhas no arquivo: {total}");
                    Console.WriteLine($"Linhas processadas: {processados}");
                    Console.WriteLine($"Linhas ignoradas (vazias): {linhasIgnoradas}");
                    Console.WriteLine($"Produtos atualizados: {atualizados}");
                    Console.WriteLine($"Campos já preenchidos (mantidos): {jaPreenchidos}");
                    Console.WriteLine($"Códigos de barras duplicados (ignorados): {duplicados}");
                    Console.WriteLine($"Produtos não encontrados no banco: {naoEncontrados}");
                    Console.WriteLine($"Erros de processamento: {erros}");

                    // Mostra avisos detalhados
                    if (avisosDetalhados.Count > 0)
                    {
                        Console.WriteLine($"\n[AVISOS - Primeiros {Math.Min(10, avisosDetalhados.Count)}]:");
                        int avisosParaMostrar = Math.Min(10, avisosDetalhados.Count);
                        for (int i = 0; i < avisosParaMostrar; i++)
                        {
                            Console.WriteLine($"  {i + 1}. {avisosDetalhados[i]}");
                        }

                        if (avisosDetalhados.Count > 10)
                        {
                            Console.WriteLine($"  ... e mais {avisosDetalhados.Count - 10} avisos.");
                        }
                    }

                    // Mostra erros detalhados
                    if (errosDetalhados.Count > 0)
                    {
                        Console.WriteLine($"\n[ERROS DETALHADOS - Primeiros {Math.Min(maxErrosExibir, errosDetalhados.Count)}]:");
                        int errosParaMostrar = Math.Min(maxErrosExibir, errosDetalhados.Count);
                        for (int i = 0; i < errosParaMostrar; i++)
                        {
                            Console.WriteLine($"  {i + 1}. {errosDetalhados[i]}");
                        }

                        if (errosDetalhados.Count > maxErrosExibir)
                        {
                            Console.WriteLine($"  ... e mais {errosDetalhados.Count - maxErrosExibir} erros.");
                        }

                        // Salva todos os erros em um arquivo
                        try
                        {
                            string erroLogPath = Path.Combine(Path.GetDirectoryName(filePath),
                                $"erros_atualizacao_{DateTime.Now:yyyyMMdd_HHmmss}.txt");
                            File.WriteAllLines(erroLogPath, errosDetalhados);
                            Console.WriteLine($"\n[Todos os erros foram salvos em:] {erroLogPath}");
                        }
                        catch
                        {
                            // Ignora erro ao salvar arquivo de log
                        }
                    }

                    // Salva avisos em arquivo separado
                    try
                    {
                        string avisosLogPath = Path.Combine(Path.GetDirectoryName(filePath),
                            $"avisos_atualizacao_{DateTime.Now:yyyyMMdd_HHmmss}.txt");
                        File.WriteAllLines(avisosLogPath, avisosDetalhados);
                        Console.WriteLine($"\n[Todos os avisos foram salvos em:] {avisosLogPath}");
                    }
                    catch
                    {
                        // Ignora erro ao salvar arquivo de log
                    }

                    if (duplicados > 0)
                    {
                        Console.WriteLine($"\n[IMPORTANTE] {duplicados} códigos de barras duplicados foram ignorados.");
                    }

                    if (jaPreenchidos > 0)
                    {
                        Console.WriteLine($"\n[INFO] {jaPreenchidos} campos já estavam preenchidos e foram mantidos.");
                    }

                    if (naoEncontrados > 0)
                    {
                        Console.WriteLine($"\n[AVISO] {naoEncontrados} IDs não encontrados na tabela produtos.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[ERRO CRÍTICO] Falha ao processar arquivo: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
            }
        }
    }
}