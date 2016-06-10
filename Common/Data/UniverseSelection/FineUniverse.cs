using System;
using System.Collections.Generic;
using System.Linq;
using QuantConnect.Securities;

namespace QuantConnect.Data.UniverseSelection
{
    /// <summary>
    /// Represents a universe based on fine fundamental data
    /// </summary>
    public class FineFundamentalUniverse : Universe
    {
        /// <summary>
        /// Specifies the delegate type used to select symbols from fine fundamental data
        /// </summary>
        /// <param name="universeSelectionData">The fine fundamental data used to select symbols from</param>
        /// <returns>The symbols passing the selection criteria</returns>
        public delegate IEnumerable<Symbol> Selector(IEnumerable<FineFundamental> universeSelectionData);

        private readonly Selector _selector;
        private readonly UniverseSettings _universeSettings;

        /// <summary>
        /// Gets the settings used for subscriptons added for this universe
        /// </summary>
        public override UniverseSettings UniverseSettings
        {
            get { return _universeSettings; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FineFundamentalUniverse"/> class
        /// </summary>
        /// <param name="symbol">The universe symbol</param>
        /// <param name="universeSettings">The settings for subscriptions produced by this universe</param>
        /// <param name="selector">The selection function used to pick symbols from the coarse input data</param>
        /// <param name="securityInitializer">An optional security intializer run on the security before receiving data.
        /// If null then the algorithm's security initializer will be used</param>
        public FineFundamentalUniverse(Symbol symbol, UniverseSettings universeSettings, Selector selector, ISecurityInitializer securityInitializer = null)
            : base(CreateConfiguration(symbol), securityInitializer)
        {
            _selector = selector;
            _universeSettings = universeSettings;
        }

        /// <summary>
        /// Performs universe selection using the data specified
        /// </summary>
        /// <param name="utcTime">The current utc time</param>
        /// <param name="data">The symbols to remain in the universe</param>
        /// <returns>The data that passes the filter</returns>
        public override IEnumerable<Symbol> SelectSymbols(DateTime utcTime, BaseDataCollection data)
        {
            return _selector(data.Data.OfType<FineFundamental>());
        }

        private static SubscriptionDataConfig CreateConfiguration(Symbol symbol)
        {
            return new SubscriptionDataConfig(typeof(FineFundamental), symbol, Resolution.Daily, TimeZones.NewYork, TimeZones.NewYork, false, false, true, isFilteredSubscription: false);
        }
    }
}
