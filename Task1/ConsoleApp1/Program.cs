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
using System.Collections.Concurrent;
using System.Threading;

namespace ConsoleApp1
{
    public class Config
    {
        public string folder_a_path { get; set; }
        public string folder_b_path { get;set; }

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
    public class ConcurrencyProperties
    {
        public int ErrorCounter { get; set; }
        public int procLineCounter { get; set; }
        public int procFileCounter { get; set; }
        public ConcurrentQueue<string> filepathes { get; set; }
        public ConcurrentBag<string> validLines { get; set; }
        public ConcurrentBag<string> invalidFilesPath { get; set; }    

        public ConcurrencyProperties()
        {
            filepathes = new ConcurrentQueue<string>();
            validLines = new ConcurrentBag<string>();
            invalidFilesPath = new ConcurrentBag<string>();
            ErrorCounter= 0; procFileCounter= 0; procLineCounter= 0;
        }

    }
    internal class Program
    {
        static void Main(string[] args)
        {
            /// Stage 1 - Reading file names
            const int tasksNum = 3;

            ConcurrencyProperties properties= new ConcurrencyProperties();
            Config jsonData = JsonSerializer.Deserialize<Config>(System.IO.File.ReadAllText("D:\\Student\\Radency\\Task1\\ConsoleApp1\\Task1\\ConsoleApp1\\config.json"));

            string cityPattern = @"'[a-z]+,\s[a-z]+\s[0-9]+,\s[0-9]+'", 
                   pattern = @"[a-z]+,[a-z]+,'[a-z]+,\s[a-z]+\s\d+,\s\d+',\d+[.]\d+,[0-9]{4}-[0-9]{2}-[0-9]{2},[0-9]+,[a-z]+";

            Task DirectoryReading = Task.Factory.StartNew(() =>
            {
                var files_name = Directory.GetFiles(jsonData.folder_a_path);
                foreach (var file in files_name)
                {
                    FileInfo fileinf = new FileInfo(file);
                    if (fileinf.Extension == ".txt" || fileinf.Extension == ".csv") {
                        properties.filepathes.Enqueue(file);
                    };

                }
            });



            //// Stage 2 - Validating
            var FileValidatingList = new List<Task>();
            for (int i = 0; i < tasksNum; i++)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(100);
                    while (properties.filepathes.TryDequeue(out var file))
                    {
                        StreamReader reader = new StreamReader(file);
                        string line;
                       
                        FileInfo fileinf = new FileInfo(file);
                        bool invalidLine = false;
                        if (fileinf.Extension == ".csv") reader.ReadLine();
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (Regex.IsMatch(line, pattern, RegexOptions.IgnoreCase))
                            {
                                properties.validLines.Add(line);
                                properties.procLineCounter++;
                            }
                            else
                            {
                                properties.ErrorCounter++;
                                invalidLine= true;
                            }
                        }
                        if (invalidLine) properties.invalidFilesPath.Add(file);
                        properties.procFileCounter++;
                        reader.Close();
                    }
                });
                FileValidatingList.Add(t);
            }
            Task.WaitAll(FileValidatingList.ToArray());


            ///Stage 3 - Transofrmation
            var database = from Line in properties.validLines
                           let toSplit = Regex.Match(Line, cityPattern, RegexOptions.IgnoreCase).Value
                           let SplitLine = Line.Replace(toSplit + ',', "").Split(',')
                           let city = toSplit.Replace("\'", "").Split(',')[0]
                           select new
                           {
                               city,
                               service_name = SplitLine[5],
                               payer = SplitLine[0] + " " + SplitLine[1],
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

            ///Stage 4 - Save
            var fileName = jsonData.folder_b_path + "\\file-"+ DateTime.Now.ToString("dd-MM-yyyy") + ".log"; 
            FileStream log = File.Create(fileName);

            StreamWriter logWriter = new StreamWriter(log);
            logWriter.Write("LOG INFO " + DateTime.Now + $"\nparsed_files: {properties.procFileCounter}\nparsed_lines: {properties.procLineCounter}\nfound_errors: {properties.ErrorCounter}\ninvalid_files: [ ");
            foreach(var path in properties.invalidFilesPath)
            {
                logWriter.Write(path + ", ");
            }
            logWriter.Write(']');
            logWriter.Flush();

            Console.ReadLine();
        }
    }
}
