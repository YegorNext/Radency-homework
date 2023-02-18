using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace ConsoleApp1
{
    public class Config
    {
        public string path { get; set; }

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
            Console.ReadLine();
        }
    }
}
