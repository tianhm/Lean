/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.IO;
using System.Linq;
using QuantConnect.Logging;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;

namespace QuantConnect.ToolBox.AlgoSeekOptionsConverter
{
    /// <summary>
    /// Process a directory of algoseek option files into separate resolutions.
    /// </summary>
    public class AlgoSeekOptionsConverter
    {
        private string _source;
        private string _destination;
        private long _flushInterval;
        private DateTime _referenceDate;
        private Resolution[] _resolutions;

        /// <summary>
        /// Create a new instance of the AlgoSeekOptions Converter. Parse a single input directory into an output.
        /// </summary>
        /// <param name="referenceDate">Datetime to be added to the milliseconds since midnight. Algoseek data is stored in channel files (XX.bz2) and in a source directory</param>
        /// <param name="source">Source directory of the .bz algoseek files</param>
        /// <param name="destination">Data directory of LEAN</param>
        /// <param name="flushInterval">How many lines should we hold in memory before flushing.</param>
        public AlgoSeekOptionsConverter(DateTime referenceDate, string source, string destination, long flushInterval = 100000000)
        {
            _source = source;
            _referenceDate = referenceDate;
            _destination = destination;
            _flushInterval = flushInterval;
        }
        
        /// <summary>
        /// Give the reference date and source directory, convert the algoseek options data into n-resolutions LEAN format.
        /// </summary>
        public void Convert(params Resolution[] resolutions)
        {
            _resolutions = resolutions;

            //Get the list of all the files, then for each file open a separate streamer.
            var files = Directory.EnumerateFiles(_source).OrderByDescending(x => new FileInfo(x).Length);
            Log.Trace("AlgoSeekOptionsConverter.Convert(): Loading {0} AlgoSeekOptionsReader for {1}...", files.Count(), _referenceDate);
            var optionsReaders = files.Select(file => new AlgoSeekOptionsReader(file, _referenceDate)).ToList();

            //Initialize parameters
            var totalLinesProcessed = 0L;
            var frontier = DateTime.MinValue;
            var updatedSymbols = new HashSet<Symbol>();
            var estimatedEndTime = _referenceDate.AddHours(16);
            var synchronizer = new SynchronizingEnumerator(optionsReaders);
            var processors = new Dictionary<Symbol, List<AlgoSeekOptionsProcessor>>();

            // Prime the synchronizer if required:
            if (synchronizer.Current == null)
            {
                synchronizer.MoveNext();
            }

            
            Log.Trace("AlgoSeekOptionsConverter.Convert(): Synchronizing and processing ticks...", files.Count(), _referenceDate);
            do
            {
                var tick = synchronizer.Current;
                frontier = tick.Time;
                updatedSymbols.Add(tick.Symbol);

                //Add or create the consolidator-flush mechanism for symbol:
                List<AlgoSeekOptionsProcessor> symbolProcessors;
                if (!processors.TryGetValue(tick.Symbol, out symbolProcessors))
                {
                    symbolProcessors = new List<AlgoSeekOptionsProcessor>(resolutions.Length);
                    foreach (var resolution in resolutions)
                    {
                        symbolProcessors.Add(new AlgoSeekOptionsProcessor(tick.Symbol, TickType.Quote, resolution, _destination));
                        symbolProcessors.Add(new AlgoSeekOptionsProcessor(tick.Symbol, TickType.Trade, resolution, _destination));
                    }
                    processors[tick.Symbol] = symbolProcessors;
                }

                // Pass current tick into processor:
                symbolProcessors = processors[tick.Symbol];
                foreach (var unit in symbolProcessors)
                {
                    unit.Process(tick);
                }

                //Due to limits on the files that can be open at a time we need to constantly flush this to disk.
                totalLinesProcessed++;
                if (totalLinesProcessed % 1000000m == 0)
                {
                    var completed = Math.Round(1 - (estimatedEndTime - frontier).TotalMinutes / TimeSpan.FromHours(6.5).TotalMinutes, 3);
                    Log.Trace("AlgoSeekOptionsConverter.Convert(): Processed {0,3}M ticks; Memory in use: {1} MB; Frontier Time: {2}; Completed: {3:P3}", Math.Round(totalLinesProcessed / 1000000m, 2), Process.GetCurrentProcess().WorkingSet64 / (1024 * 1024), frontier.ToString("u"), completed);
                }

                if (totalLinesProcessed % _flushInterval == 0)
                {
                    Log.Trace("AlgoSeekOptionsConverter.Convert(): Writing memory buffer of {0} symbols to disk...", updatedSymbols.Count);
                    foreach (var updated in updatedSymbols)
                    {
                        processors[updated].ForEach(x => x.FlushBuffer(frontier));
                    }
                    updatedSymbols.Clear();
                }
            }
            while (synchronizer.MoveNext());

            Log.Trace("AlgoSeekOptionsConverter.Convert(): Performing final flush to disk... ");
            foreach (var symbol in processors.Keys)
            {
                processors[symbol].ForEach(x => x.FlushBuffer(DateTime.MaxValue, true));
            }

            Log.Trace("AlgoSeekOptionsConverter.Convert(): Finished processing directory: " + _source);
        }

        /// <summary>
        /// Point to a directory of option csv files and compress into a single option.zip for the day.
        /// </summary>
        /// <param name="dataDirectory">Directory with the option csv files</param>
        /// <param name="parallelism">Number of threads to use in the compression, defaults to 1</param>
        public void Compress(string dataDirectory, int parallelism = 1)
        {
            Log.Trace("AlgoSeekOptionsConverter.Compress(): Begin compressing csv files");

            var root = Path.Combine(dataDirectory, "option", "usa");
            var fine =
                from res in _resolutions
                let path = Path.Combine(root, res.ToLower())
                from sym in Directory.EnumerateDirectories(path)
                from dir in Directory.EnumerateDirectories(sym)
                select new DirectoryInfo(dir).FullName;

            var options = new ParallelOptions {MaxDegreeOfParallelism = parallelism};
            Parallel.ForEach(fine, options, dir =>
            {
                try
                {
                    // zip the contents of the directory and then delete the directory
                    Compression.ZipDirectory(dir, dir + ".zip", false);
                    Directory.Delete(dir, true);
                    Log.Trace("Processed: " + dir);
                }
                catch (Exception err)
                {
                    Log.Error(err, "Zipping " + dir);
                }
            });
        }   
    }
}