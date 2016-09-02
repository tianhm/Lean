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
using QuantConnect.Data;
using System.Collections.Generic;
using System.Linq;
using QuantConnect.Data.Consolidators;
using QuantConnect.Data.Market;
using QuantConnect.Logging;

namespace QuantConnect.ToolBox.AlgoSeekOptionsConverter
{
    /// <summary>
    /// Processor for caching and consolidating ticks; 
    /// then flushing the ticks in memory to disk when triggered.
    /// </summary>
    public class AlgoSeekOptionsProcessor
    {
        private Symbol _symbol;
        private TickType _tickType;
        private Resolution _resolution;
        private Queue<BaseData> _queue; 
        private string _destinationDirectory;
        private IDataConsolidator _consolidator;

        /// <summary>
        /// Create a new AlgoSeekOptionsProcessor for enquing consolidated bars and flushing them to disk
        /// </summary>
        /// <param name="symbol">Symbol for the processor</param>
        /// <param name="tickType">TradeBar or QuoteBar to generate</param>
        /// <param name="resolution">Resolution to consolidate</param>
        /// <param name="destinationDirectory">Data directory for LEAN</param>
        public AlgoSeekOptionsProcessor(Symbol symbol, TickType tickType, Resolution resolution, string destinationDirectory)
        {
            _symbol = symbol;
            _tickType = tickType;
            _resolution = resolution;
            _queue = new Queue<BaseData>();
            _destinationDirectory = destinationDirectory;

            // Setup the consolidator for the requested resolution
            _consolidator = new PassthroughConsolidator();
            if (resolution != Resolution.Tick)
            {
                if (tickType == TickType.Trade)
                {
                    _consolidator = new TickConsolidator(resolution.ToTimeSpan());
                }
                else
                {
                    _consolidator = new TickQuoteBarConsolidator(resolution.ToTimeSpan());
                }
            }
            
            // On consolidating the bars put the bar into a queue in memory to be written to disk later.
            _consolidator.DataConsolidated += (sender, consolidated) =>
            {
                _queue.Enqueue(consolidated);
            };
        }

        /// <summary>
        /// Process the tick; add to the con
        /// </summary>
        /// <param name="data"></param>
        public void Process(BaseData data)
        {
            if (((Tick)data).TickType != _tickType)
            {
                return;
            }

            _consolidator.Update(data);
        }

        /// <summary>
        /// Write the in memory queues to the disk.
        /// </summary>
        /// <param name="frontierTime">Current foremost tick time</param>
        /// <param name="finalFlush">Indicates is this is the final push to disk at the end of the data</param>
        public void FlushBuffer(DateTime frontierTime, bool finalFlush = false)
        {
            //Force the consolidation if time has past the bar
            _consolidator.Scan(frontierTime);

            // If this is the final packet dump it to the queue
            if (finalFlush && _consolidator.WorkingData != null)
            {
                _queue.Enqueue(_consolidator.WorkingData);
            }

            // Purge the queue to disk if there's work to do:
            if (_queue.Count == 0) return;
            using (var writer = new LeanOptionsWriter(_destinationDirectory, _symbol, frontierTime, _resolution, _tickType))
            {
                while (_queue.Count > 0)
                {
                    writer.WriteEntry(_queue.Dequeue());
                }
            }
        }
    }

    /// <summary>
    /// This is a shim for handling Tick resolution data in TickRepository
    /// Ordinary TickConsolidators presents Consolidated data as type TradeBars.
    /// However, LeanData.GenerateLine expects Tick resolution data to be of type Tick.
    /// This class lets tick data pass through without changing object type,
    /// which simplifies the logic in TickRepository.
    /// </summary>
    internal class PassthroughConsolidator : IDataConsolidator
    {
        public BaseData Consolidated { get; private set; }

        public BaseData WorkingData
        {
            get { return null; }
        }

        public Type InputType
        {
            get { return typeof(BaseData); }
        }

        public Type OutputType
        {
            get { return typeof(BaseData); }
        }

        public event DataConsolidatedHandler DataConsolidated;

        public void Update(BaseData data)
        {
            Consolidated = data;
            if (DataConsolidated != null)
            {
                DataConsolidated(this, data);
            }
        }

        public void Scan(DateTime currentLocalTime)
        {
        }
    }
}