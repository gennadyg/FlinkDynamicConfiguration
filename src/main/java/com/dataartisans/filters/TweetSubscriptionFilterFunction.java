package com.dataartisans.filters;

import com.dataartisans.domain.Customer;
import com.dataartisans.domain.CustomerImpression;
import com.dataartisans.domain.TweetImpression;
import com.dataartisans.domain.TweetSubscription;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.LoadingCache;

/**
 * This function consumes a connected stream.  The two individual streams are a stream of TweetImpressions
 * and a stream of TweetSubscriptions.  The TweetSubscription indicates that a customer would
 * like to receive TweetImpressions for the given tweet.  For each TweetImpression consumed this function
 * emits a message for *each* customer that has subscribed to that tweet.
 */
public class TweetSubscriptionFilterFunction extends RichCoFlatMapFunction<TweetImpression, TweetSubscription, CustomerImpression> {

    private static Logger LOG = LoggerFactory.getLogger(TweetSubscriptionFilterFunction.class);

    private transient ValueState<Tuple2<Customer, String>> formats;

    private ListStateDescriptor<Customer> tweetSubscriptionsStateDesc =
            new ListStateDescriptor<>("tweetSubscriptions", Customer.class);

    @Override
    public void flatMap1( TweetImpression impression, Collector<CustomerImpression> out) throws Exception {

        Iterable<Customer> customers = getRuntimeContext().getListState( tweetSubscriptionsStateDesc).get();

        ValueStateDescriptor<Tuple2<Customer, String>> descriptor =
                    new ValueStateDescriptor<>(
                            impression.getTweetId().toString(), // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Customer, String>>() {}), // type information
                            Tuple2.of( null, ""));
            Tuple2<Customer, String> customerFormats = getRuntimeContext().getState( descriptor ).value();

            if( customerFormats.f0 != null ){ // found customer settings

                Customer foundCustomer = customerFormats.f0;
                LOG.info( customerFormats.f1 );

                out.collect( new CustomerImpression( foundCustomer, impression));
            }
    }

    @Override
    public void flatMap2( TweetSubscription subscription, Collector<CustomerImpression> out ) throws Exception {

        ValueStateDescriptor<Tuple2<Customer, String>> descriptor =
                new ValueStateDescriptor<>(
                        subscription.getTweetId().toString(), // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Customer, String>>() {}), // type information
                        Tuple2.of( null, "")); // default value of the state, if nothing was set

        formats = getRuntimeContext().getState( descriptor );

        Tuple2<Customer, String> currentFormats = formats.value();
        currentFormats.f0 = subscription.getCustomer();
        currentFormats.f1 = subscription.getCustomer().timeFormat;
        formats.update( currentFormats );

        LOG.info("Sub-task {}: updated subscription  delivery of tweet[{}] to customer[{}] with format {}", getRuntimeContext().getIndexOfThisSubtask() + 1, subscription.getTweetId(), subscription.getCustomer(), subscription.getCustomer().timeFormat );

    }
}
