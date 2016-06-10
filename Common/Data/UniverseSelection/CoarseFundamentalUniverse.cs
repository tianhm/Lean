using System;
using System.Collections.Generic;
using System.Linq;
using QuantConnect.Securities;

namespace QuantConnect.Data.UniverseSelection
{
    /// <summary>
    /// Represents a universe based on coarse fundamental data
    /// </summary>
    public class CoarseFundamentalUniverse : Universe
    {
        /// <summary>
        /// Specifies the delegate type used to select symbols from coarse fundamental data
        /// </summary>
        /// <param name="universeSelectionData">The coarse fundamental data used to select symbols from</param>
        /// <returns>The symbols passing the selection criteria</returns>
        public delegate IEnumerable<Symbol> Selector(IEnumerable<CoarseFundamental> universeSelectionData);

        private readonly Selector _selector;
        private readonly UniverseSettings _universeSettings;

        /// <summary>
        /// When non-null, this universe will emit configuratins uses this as a template (everything except symbol)
        /// </summary>
        /// <remarks>
        /// Non-ideal, decorator pattern would be nice, but we have some type checking, so too risky for now
        /// </remarks>
        public SubscriptionDataConfig ConfigurationRedirect
        {
            get; set;
        }

        /// <summary>
        /// Gets the settings used for subscriptons added for this universe
        /// </summary>
        public override UniverseSettings UniverseSettings
        {
            get { return _universeSettings; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CoarseFundamentalUniverse"/> class
        /// </summary>
        /// <param name="universeSymbol">The symbol for this universe</param>
        /// <param name="universeSettings">Settings used when creating new subscriptions</param>
        /// <param name="selector">The selection function used to pick symbols from the coarse input data</param>
        /// <param name="securityInitializer">An optional security intializer run on the security before receiving data.
        /// If null then the algorithm's security initializer will be used</param>
        public CoarseFundamentalUniverse(Symbol universeSymbol, UniverseSettings universeSettings, Selector selector, ISecurityInitializer securityInitializer = null)
            : base(CreateConfiguration(universeSymbol), securityInitializer)
        {
            _selector = selector;
            _universeSettings = universeSettings;
        }
        
        /// <summary>
        /// Performs universe selection using the data specified data
        /// </summary>
        /// <param name="utcTime">The current utc time</param>
        /// <param name="data">The symbols to remain in the universe</param>
        /// <returns>The data that passes the filter</returns>
        public override IEnumerable<Symbol> SelectSymbols(DateTime utcTime, BaseDataCollection data)
        {
            return _selector(data.Data.OfType<CoarseFundamental>());
        }

        /// <summary>
        /// Gets the subscription configs for the specified security
        /// </summary>
        /// <remarks>
        /// In most cases the default implemention of returning the security's configuration is
        /// sufficient. It's when we want multiple subscriptions (trade/quote data) that we'll need
        /// to override this
        /// </remarks>
        /// <param name="security">The security to get subscriptions for</param>
        /// <returns>All subscriptions required by this security</returns>
        protected override IEnumerable<SubscriptionDataConfig> GetSubscriptionConfigurations(Security security)
        {
            if (ConfigurationRedirect != null)
            {
                return base.GetSubscriptionConfigurations(security).Select(config =>
                    new SubscriptionDataConfig(ConfigurationRedirect, symbol: config.Symbol)
                    );
            }

            return base.GetSubscriptionConfigurations(security);
        }

        private static SubscriptionDataConfig CreateConfiguration(Symbol symbol)
        {
            return new SubscriptionDataConfig(typeof(CoarseFundamental), symbol, Resolution.Daily, TimeZones.NewYork, TimeZones.NewYork, false, false, true, isFilteredSubscription: false);
        }
    }
}
