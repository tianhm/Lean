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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using QuantConnect.Data.Market;

namespace QuantConnect.ToolBox.AlgoSeekOptionsConverter
{
    /// <summary>
    /// Enumerator for converting AlgoSeek option files into Ticks.
    /// </summary>
    public class AlgoSeekOptionsReader : IEnumerator<Tick>
    {
        private DateTime _date;
        private Stream _stream;
        private StreamReader _streamReader;

        /// <summary>
        /// Enumerate through the lines of the algoseek files.
        /// </summary>
        /// <param name="file">BZ File for algoseek</param>
        /// <param name="date">Reference date of the folder</param>
        public AlgoSeekOptionsReader(string file, DateTime date)
        {
            _date = date;
            var streamProvider = StreamProvider.ForExtension(Path.GetExtension(file));
            _stream = streamProvider.Open(file).First();
            _streamReader = new StreamReader(_stream);

            //Prime the data pump, set the current.
            Current = null;
            MoveNext();
        }

        /// <summary>
        /// Parse the next line of the algoseek option file.
        /// </summary>
        /// <returns></returns>
        public bool MoveNext()
        {
            string line;
            Tick tick = null;
            while ((line = _streamReader.ReadLine()) != null && tick == null)
            {
                // If line is invalid continue looping to find next valid line.
                tick = Parse(line);
            }
            Current = tick;
            return Current != null;
        }

        /// <summary>
        /// Current top of the tick file.
        /// </summary>
        public Tick Current
        {
            get; private set; 
            
        }

        /// <summary>
        /// Gets the current element in the collection.
        /// </summary>
        /// <returns>
        /// The current element in the collection.
        /// </returns>
        object IEnumerator.Current
        {
            get { return Current; }
        }

        /// <summary>
        /// Reset the enumerator for the AlgoSeekOptionReader
        /// </summary>
        public void Reset()
        {
            throw new NotImplementedException("Reset not implemented for AlgoSeekOptionsReader.");
        }

        /// <summary>
        /// Dispose of the underlying AlgoSeekOptionsReader
        /// </summary>
        public void Dispose()
        {
            _stream.Close();
            _stream.Dispose();
            _streamReader.Close();
            _streamReader.Dispose();
        }
        
        /// <summary>
        /// Parse a string line into a option tick.
        /// </summary>
        /// <param name="line"></param>
        /// <returns></returns>
        private Tick Parse(string line)
        {
            // filter out bad lines as fast as possible
            EventType eventType;
            if (!EventType.TryParse(line, out eventType))
            {
                return null;
            }

            // parse csv check column count
            const int columns = 11;
            var csv = line.ToCsv(columns);
            if (csv.Count < columns)
            {
                return null;
            }

            // ignoring time zones completely -- this is all in the 'data-time-zone'
            var timeString = csv[0];
            var hours = timeString.Substring(0, 2).ToInt32();
            var minutes = timeString.Substring(3, 2).ToInt32();
            var seconds = timeString.Substring(6, 2).ToInt32();
            var millis = timeString.Substring(9, 3).ToInt32();
            var time = _date.Add(new TimeSpan(0, hours, minutes, seconds, millis));

            // detail: PUT at 30.0000 on 2014-01-18
            var underlying = csv[4];

            var optionRight = csv[5][0] == 'P' ? OptionRight.Put : OptionRight.Call;
            var expiry = DateTime.ParseExact(csv[6], "yyyyMMdd", null);
            var strike = csv[7].ToDecimal() / 10000m;
            var optionStyle = OptionStyle.American; // couldn't see this specified in the file, maybe need a reference file
            var sid = SecurityIdentifier.GenerateOption(expiry, underlying, Market.USA, strike, optionRight, optionStyle);
            var symbol = new Symbol(sid, underlying);

            var price = csv[9].ToDecimal() / 10000m;
            var quantity = csv[8].ToInt32();

            var tick = new Tick
            {
                Symbol = symbol,
                Time = time,
                TickType = eventType.TickType,
                Exchange = csv[10],
                Value = price
            };
            if (eventType.TickType == TickType.Quote)
            {
                if (eventType.IsAsk)
                {
                    tick.AskPrice = price;
                    tick.AskSize = quantity;
                }
                else
                {
                    tick.BidPrice = price;
                    tick.BidSize = quantity;
                }
            }
            else
            {
                tick.Quantity = quantity;
            }

            return tick;
        }
    }

    /// <summary>
    /// Classify the option tick from AlgoSeek
    /// </summary>
    class EventType
	{
		public static readonly EventType Trade = new EventType(false, TickType.Trade);
		public static readonly EventType Bid = new EventType(false, TickType.Quote);
		public static readonly EventType Ask = new EventType(true, TickType.Quote);

		public bool IsAsk { get; private set; }
		public TickType TickType { get; private set; }

		private EventType(bool isAsk, TickType tickType)
		{
			IsAsk = isAsk;
			TickType = tickType;
		}

		public static bool TryParse(string line, out EventType eventType)
		{
			switch (line[13])
			{
			case 'T':
				eventType = Trade;
				break;
			case 'F':
				switch (line[15])
				{
				case 'B':
					eventType = Bid;
					break;
				case 'O':
					eventType = Ask;
					break;
				default:
					eventType = null;
					return false;
				}
				break;
			default:
				eventType = null;
				return false;
			}

			return true;
		}
	}
}
