using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantConnect.Data.UniverseSelection
{
    public class FineFundamental : BaseData
    {

        /// <summary>
        /// Creates the symbol used for coarse fundamental data
        /// </summary>
        /// <param name="market">The market</param>
        /// <returns>A coarse universe symbol for the specified market</returns>
        public static Symbol CreateUniverseSymbol(string market)
        {
            market = market.ToLower();
            var ticker = "qc-universe-fine-" + market;
            var sid = SecurityIdentifier.GenerateEquity(SecurityIdentifier.DefaultDate, ticker, market);
            return new Symbol(sid, ticker);
        }
    }
}
