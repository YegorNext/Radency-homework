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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Windows.Markup;

namespace ConsoleApp1
{
    public class Config
    {
        public string path { get; set; }

    }

    public class TransactionData
    {
        public string city { get; set; }
        public IEnumerable<Services> services { get; set; }
        public decimal total { get; set; }
    }
    public class Services
    {
        public string name { get; set; }
        public IEnumerable<Payers> payers{ get; set; }

        public decimal total { get; set; }
    }
    public class Payers
    {
        public string name { get; set; }
        public decimal payment { get; set; }
        public string date { get; set; }
        public long account_number { get; set; }
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


            string[] str = new string[6]; string patt = @"'[a-z]+,\s[a-z]+\s[0-9]+,\s[0-9]+'";
            str[0] = "John,Doe,'Lviv, Kleparivska 35, 4',500.0,2022-27-01,1234567,Water";
            str[1] = "Mark,Doe,'Lviv, Kleparivska 35, 4',1250.35,2022-27-01,1234567,Gaz";
            str[2] = "Charlz,Doe,'Lviv, Kleparivska 35, 4',1250.35,2022-27-01,1234567,Gaz";
            str[3] = "Carl,Doe,'Pavlograd, Kleparivska 35, 4',1250.35,2022-27-01,1234567,Gaz";
            str[4] = "Sem,Doe,'Pavlograd, Kleparivska 35, 4',600.35,2022-27-01,1234567,Gaz";
            str[5] = "Shara,Doe,'Pavlograd, Kleparivska 35, 4',250.35,2022-27-01,1234567,Water";


            var database = from Line in str
                           let toSplit = Regex.Match(Line, patt, RegexOptions.IgnoreCase).Value
                           let SplitLine = Line.Replace(toSplit + ',', "").Split(',')
                           let city = toSplit.Replace("\'", "").Split(',')[0]
                           select new
                           {
                               city,
                               service_name = SplitLine[5],
                               payer = SplitLine[0] + "" + SplitLine[1],
                               payment = decimal.Parse(SplitLine[2].Replace(".", ",")),
                               date = SplitLine[3],
                               account_num = long.Parse(SplitLine[4])
                           };

            var query =
                from values in database
                group values by values.city into g

                select new TransactionData
                {
                    city = g.Key,
                    services = (from srvc in database
                                where srvc.city == g.Key 
                                group srvc by srvc.service_name into h
                                select new Services
                                {
                                    name = h.Key,
                                    payers = (from pymnt in database 
                                    where pymnt.service_name == h.First().service_name && pymnt.city == h.First().city
                                    select new Payers{ name = pymnt.payer, payment = pymnt.payment, date = pymnt.date, account_number = pymnt.account_num }  
                                    ),
                                    total = h.Sum(x => x.payment)
                                }
                               ),
                    total = g.Sum(x=> x.payment)
                };
            
            var options = new JsonSerializerOptions { WriteIndented = true };
            string jsonString = JsonSerializer.Serialize(query, options);
            Console.WriteLine(jsonString);

            Console.ReadLine();
        }
    }
}
