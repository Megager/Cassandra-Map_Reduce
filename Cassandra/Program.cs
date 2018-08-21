using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Cassandra;
using Cassandra.Cassandra.Entity;

namespace Cassandra
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            var repository = new Repository<SaleOrder>();

            var query = "SELECT COUNT(*) as washington_count FROM employes WHERE city = 'Washington';";
            var europeSales = repository.ExecuteQuery(query);
            Console.WriteLine($"Query: {query}");
            Console.WriteLine("Result:");
            Console.WriteLine(europeSales.FirstOrDefault()?.GetValue<long>("washington_count").ToString());

            Console.WriteLine();

            query = "SELECT id, user_name, MAX(salary) as max_salary FROM employes;";
            var biggestSale = repository.ExecuteQuery(query).FirstOrDefault();
            Console.WriteLine($"Query: {query}");
            Console.WriteLine("Result:");
            if (biggestSale != null)
            {
                Console.WriteLine($"Id: {biggestSale.GetValue<string>("id")}");
                Console.WriteLine($"User name: {biggestSale.GetValue<string>("user_name")}");
                Console.WriteLine($"Max salary: {biggestSale.GetValue<double>("max_salary")}");
            }
            else
            {
                Console.WriteLine("Null");
            }

            Console.WriteLine();

            query = "SELECT * FROM employes WHERE age_in_company > 5 and age < 30 LIMIT 5 ALLOW FILTERING;";
            var sales = repository.ExecuteQuery(query);
            Console.WriteLine($"Query: {query}");
            Console.WriteLine("Result:");
            foreach (var sale in sales)
            {
                Console.WriteLine("****************");
                Console.WriteLine($"Id: {sale.GetValue<string>("id")}");
                Console.WriteLine($"First name: {sale.GetValue<string>("first_name")}");
                Console.WriteLine($"Last name: {sale.GetValue<string>("last_name")}");
                Console.WriteLine($"Age: {sale.GetValue<double>("age")}");
                Console.WriteLine($"Age in company: {sale.GetValue<double>("age_in_company")}");
                Console.WriteLine($"Salary: {sale.GetValue<double>("salary")}");
            }

            Console.WriteLine();
            Console.WriteLine("Start to mapping by region and aggregating by total profit...");
            var mapedValues = MapReduce();
            Console.WriteLine("Mapped by region and aggregated by total profit results:");
            foreach (var result in mapedValues)
            {
                Console.WriteLine($"Region: {result.Key} ----- Total profit: {result.Value}");
            }
            Console.WriteLine();

            var region = "Asia";
            Console.WriteLine($"Results for Region {region} ----- Total profit {mapedValues[region]}");

            Console.ReadLine();
        }

        private static Dictionary<string, decimal> MapReduce()
        {
            var mappedSales = GetMappedByRegion();
            return ReduceByTotalProfit(mappedSales);
        }

        private static Dictionary<string, List<SaleOrder>> GetMappedByRegion()
        {
            var result = new Dictionary<string, List<SaleOrder>>();
            var repository = new Repository<SaleOrder>();
            Parallel.ForEach(
                repository.GetAll(),
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                () => new Dictionary<string, List<SaleOrder>>(),
                (sale, state, index, localDic) =>
                {
                    Map(localDic, sale);
                    return localDic;
                },
                localDic =>
                {
                    lock (result)
                    {
                        foreach (var pair in localDic)
                        {
                            var key = pair.Key;
                            if (result.ContainsKey(key))
                            {
                                result[key].AddRange(pair.Value);
                            }
                            else
                            {
                                result[key] = pair.Value;
                            }
                        }
                    }
                }
            );

            return result;
        }

        private static Dictionary<string, decimal> ReduceByTotalProfit(Dictionary<string, List<SaleOrder>> mappedSales)
        {
            return mappedSales.Select(Aggregate)
                              .OrderByDescending(r => r.Value)
                              .ToDictionary(r => r.Key, r => r.Value);
        }

        private static KeyValuePair<string, decimal> Aggregate(KeyValuePair<string, List<SaleOrder>> mappedSales)
        {
            var region = mappedSales.Key;
            var totalProfitOfRegion = mappedSales.Value.Aggregate(decimal.Zero, 
                                                                  (result, sale) => decimal.Add(result, (decimal)sale.Total_Profit));

            return new KeyValuePair<string, decimal>(region, totalProfitOfRegion);
        }

        private static void Map(IDictionary<string, List<SaleOrder>> dictionary, SaleOrder saleOrder)
        {
            if (dictionary.ContainsKey(saleOrder.Region))
            {
                dictionary[saleOrder.Region].Add(saleOrder);
                return;
            }

            dictionary.Add(saleOrder.Region, new List<SaleOrder>{ saleOrder });
        }
    }
}
