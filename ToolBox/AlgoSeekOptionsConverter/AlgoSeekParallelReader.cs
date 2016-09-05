
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using QuantConnect.Data.Market;
using QuantConnect.Logging;

namespace QuantConnect.ToolBox.AlgoSeekOptionsConverter
{
    /// <summary>
    /// Take an enumerable; run it in a parallel thread with a buffer to hold the processed output.
    /// </summary>
    class AlgoSeekParallelReader : IEnumerator<Tick>
    {
        private Tick _last;
        private Thread _thread;
        private volatile bool _completed;
        private IEnumerator<Tick> _source;
        private ConcurrentQueue<Tick> _buffer;
        private object _moveLock = new object();
        private readonly AutoResetEvent _collectionFull = new AutoResetEvent(false);

        /// <summary>
        /// Given a source; enumerate the data into a queue on a parallel thread.
        /// </summary>
        /// <param name="source">Source to enumerate</param>
        /// <param name="bufferMax">Maximum size of the buffer of ticks</param>
        public AlgoSeekParallelReader(IEnumerator<Tick> source, int bufferMax = 1000000)
        {
            _source = source;
            _buffer = new ConcurrentQueue<Tick>();

            //If there's already a tick primed; catch it before starting thread.
            if (_source.Current != null)
            {
                _buffer.Enqueue(source.Current);
            }

            //Launch a parallel thread to process the file's option data.
            _thread = new Thread(() =>
            {
                while (_source.Current != null)
                {
                    //Wait for a few dequeues before continuing.
                    if (_buffer.Count > bufferMax)
                    {
                        _collectionFull.WaitOne();
                        //Thread.Yield();
                        continue;
                    }

                    lock (_moveLock)
                    {
                        if (_source.MoveNext())
                        {
                            _buffer.Enqueue(source.Current);
                        }
                    }
                }
                _completed = true;
            });
            _thread.Start();
        }

        /// <summary>
        /// Stop the thread and dispose of the stream readers.
        /// </summary>
        public void Dispose()
        {
            _thread.Abort();
        }

        /// <summary>
        /// End iteration when the buffer's empty and the source is completed.
        /// </summary>
        /// <returns></returns>
        public bool MoveNext()
        {
            _last = null;
            if (_completed && _buffer.IsEmpty)
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Threaded buffer reader has no reset.
        /// </summary>
        public void Reset()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Return items extracted from the tick queue:
        /// </summary>
        public Tick Current
        {
            get
            {
                //Return the last "current" until move next called.
                if (_last != null)
                {
                    return _last;
                }

                // Buffer empty: pull the move next from the primary sync thread to avoid waiting.
                if (_buffer.IsEmpty && !_completed)
                {
                    lock (_moveLock)
                    {
                        if (_buffer.IsEmpty)
                        {
                            _source.MoveNext();
                            _last = _source.Current;
                            return _last;
                        }
                    }
                }

                // OK to avoid safety checks here since we know only ONE consumer of the queue. 
                // (With multiple consumers TryDequeue could potentially fail);
                Tick data;
                if (_buffer.TryDequeue(out data))
                {
                    _collectionFull.Set();
                    _last = data;
                    return data;
                }

                //Buffer empty; completed true; no more ticks to offer.
                Log.Error("AlgoSeekParallelReader.Current(): Returning null early.");
                return null;
            }
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }
    }
}
