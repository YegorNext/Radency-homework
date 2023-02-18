using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;

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
            Config jsonData = JsonSerializer.Deserialize<Config>(System.IO.File.ReadAllText("D:\\Student\\Radency\\Task1\\ConsoleApp1\\Task1\\ConsoleApp1\\config.json")); 
            
            var files_name = Directory.GetFiles(jsonData.path);
            foreach (var file in files_name)
            {
                FileInfo fileinf = new FileInfo(file);
                if (fileinf.Extension == ".txt" || fileinf.Extension == ".csv") Console.WriteLine(fileinf.Name + fileinf.Extension);

            }
            Console.ReadLine();
        }
    }
}
