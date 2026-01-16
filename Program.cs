using System;
using System.IO;
using MigrationProject.Services;

namespace MigrationProject
{
    class Program
    {
        static string connectionString = "Host=localhost;Port=5432;Database='pluspdv_migration';Username='postgres';Password='admg2'";
        static string baseFolder = @"C:\Migration";

        static void Main(string[] args)
        {
            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

            Console.WriteLine("=== INICIANDO MIGRAÇÃO AUTOMÁTICA ===");
            Console.WriteLine($"Pasta dos arquivos: {baseFolder}\n");

            try
            {
                //////// 1. Clientes
                //new ClienteService(connectionString)
                //   .ImportarClientes(Path.Combine(baseFolder, "Clientes.csv"));

                //Console.WriteLine("---------------------------------------------------------");

                /////// 2. Fornecedores
                //new FornecedorService(connectionString)
                //   .ImportarFornecedores(Path.Combine(baseFolder, "Fornecedores.csv"));

                //Console.WriteLine("---------------------------------------------------------");

                /////// 3. Produtos (Cadastro Básico)
                //new ProdutoService(connectionString)
                //    .ImportarProdutos(Path.Combine(baseFolder, "Produtos.csv"));

                //Console.WriteLine("---------------------------------------------------------");

                //////// 4. Produtos (Atualizar Preços)
                //new ProdutoService(connectionString)
                //   .AtualizarPrecos(Path.Combine(baseFolder, "ProdutosPrecos.csv"));

                //Console.WriteLine("---------------------------------------------------------");

                //////// 5. Produtos (Atualizar Código de Barras e Referência) - NOVO!
                // O arquivo deve ter as colunas: Id_PRO, Nome_PRO, EAN, Referencia_Fabrica
                new ProdutoUpdateService(connectionString)
                   .AtualizarCodigoBarrasEReferencia(
                       Path.Combine(baseFolder, "AtualizarCod.csv")
                   );

                Console.WriteLine("---------------------------------------------------------");

                ///6. Financeiro (Contas a Receber)
                //new FinanceiroService(connectionString)
                //   .ExecutarMigracao(
                //       Path.Combine(baseFolder, "Clientes.csv"),
                //        Path.Combine(baseFolder, "ContasReceber.csv")
                //    );

                Console.WriteLine("\n\n>>> PROCESSO FINALIZADO! <<<");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\nERRO FATAL: {ex.Message}");
                Console.ResetColor();
            }

            Console.WriteLine("Pressione qualquer tecla para sair...");
            Console.ReadKey();
        }
    }
}