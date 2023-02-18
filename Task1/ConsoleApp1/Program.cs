using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using System.Web;

namespace ConsoleApp1
{
    public class Config
    {
        public string path { get; set; }

    }
    public class TransactionData
    {
        public string city { get; set; }
        public IList<Services> services{ get; set; }
    }
    public class Services
    {
        public string name { get; set; }
        public IList<Payers> payers { get; set; }

    }
    public class Payers
    {
        public string name { get; set; }
        public decimal payment { get; set; }
        public string date { get; set; }
        public long account_number { get; set; }
    }
    public class FileProcessTransfer
    {
        private string line;
        private string[] package;

        public string Line
        {
            get { return line; }
        }
        FileProcessTransfer(string line)
        {
            this.line = line;
            package = new string[5];
        }

        private void sepData()
        {

        }
    }
    internal class Program
    {
        static void Main(string[] args)
        {
            /// Stage 1 - Reading file names
            Config jsonData = JsonSerializer.Deserialize<Config>(System.IO.File.ReadAllText("D:\\Student\\Radency\\Task1\\ConsoleApp1\\Task1\\ConsoleApp1\\config.json")); 
            
            var files_name = Directory.GetFiles(jsonData.path);
            foreach (var file in files_name)
            {
                FileInfo fileinf = new FileInfo(file);
                if (fileinf.Extension == ".txt" || fileinf.Extension == ".csv") Console.WriteLine(fileinf.Name + fileinf.Extension);

            }


            //// Stage 2 - Validating
            StreamReader reader = new StreamReader("D:\\Student\\Radency\\Task1\\ConsoleApp1\\Task1\\Data\\t1_c1.txt");
            string line, pattern = @"[a-z]+,[a-z]+,'[a-z]+,\s[a-z]+\s\d+,\s\d+',\d+[.]\d+,[0-9]{4}-[0-9]{2}-[0-9]{2},[0-9]+,[a-z]+";
            
            while((line = reader.ReadLine()) != null)
             {
                Console.WriteLine(Regex.IsMatch(line, pattern, RegexOptions.IgnoreCase));
            }
            reader.Close();

            ///Stage 3 - Transofrmation
            var data = new TransactionData
            {
                city = "Dnipro",
                services = new List<Services>()
                {
                    new Services() {name = "Gaz", payers = new List<Payers>(){ 
                        new Payers(){name = "John", payment = Convert.ToDecimal(535.5), date = "353", account_number = 2352562}
                    } }
                }
            };

            var options = new JsonSerializerOptions { WriteIndented = true };
            string jsonString = JsonSerializer.Serialize(data, options);
            Console.WriteLine(jsonString);

            Console.ReadLine();
        }
    }
}
