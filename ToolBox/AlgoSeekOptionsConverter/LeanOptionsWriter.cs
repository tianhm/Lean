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
using QuantConnect.Data;
using QuantConnect.Util;

namespace QuantConnect.ToolBox.AlgoSeekOptionsConverter
{
    /// <summary>
    /// Create an options data writier and put entries into individual option .csv files.
    /// </summary>
    internal class LeanOptionsWriter : IDisposable
    {
        private Resolution _resolution;
        private StreamWriter _streamWriter;
        
        /// <summary>
        /// Create a new instance of a LeanOptionDataWriter.
        /// </summary>
        public LeanOptionsWriter(string dataDirectory, Symbol symbol, DateTime date, Resolution resolution, TickType tickType)
        {
            //Create a folder to store all the csv's in temporarily until zipped.
            var entry = LeanData.GenerateZipEntryName(symbol, date, resolution, tickType);
            var relativePath = LeanData.GenerateRelativeZipFilePath(symbol, date, resolution, tickType).Replace(".zip", string.Empty);
            var path = Path.Combine(Path.Combine(dataDirectory, relativePath), entry);
            var directory = new FileInfo(path).Directory.FullName;
            Directory.CreateDirectory(directory);

            _streamWriter = new StreamWriter(path);
            _resolution = resolution;
        }

        /// <summary>
        /// Write this line to the existing csv file we have open.
        /// </summary>
        /// <param name="data">Data to write.</param>
        public void WriteEntry(BaseData data)
        {
            var line = LeanData.GenerateLine(data, data.Symbol.ID.SecurityType, _resolution);
            _streamWriter.WriteLine(line);
        }

        /// <summary>
        /// Wrapper to write the line rather than a whole basedata. Save memory on storing objects.
        /// </summary>
        /// <param name="line"></param>
        public void WriteEntry(string line)
        {
            _streamWriter.WriteLine(line);
        }

        /// <summary>
        /// Flush the stream writer.
        /// </summary>
        public void Flush()
        {
            _streamWriter.Flush();
        }

        /// <summary>
        /// Dispose of the internals
        /// </summary>
        public void Dispose()
        {
            _streamWriter.Dispose();
        }
    }
}