package com.dataartisans.sinks;

import com.dataartisans.domain.Customer;
import com.dataartisans.domain.CustomerImpression;
import com.dataartisans.utils.TimeUtils;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This sink is for messages to be delivered to one particular customer.
 */
public class CustomerSinkFunction extends RichSinkFunction<CustomerImpression> {
  private static Logger LOG = LoggerFactory.getLogger(CustomerSinkFunction.class);



  private final Customer customer;

  public CustomerSinkFunction(Customer customer){
    this.customer = customer;
  }

  @Override
  public void invoke( CustomerImpression impression) throws Exception {



    LOG.info("Delivering {} to sink for {} time in customer format {}", impression.getTweetImpression(), customer, TimeUtils.currentDateToCustomFormat( impression.getCustomer().timeFormat ));
  }
}
