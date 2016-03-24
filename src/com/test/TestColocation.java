package com.test;

import java.io.Serializable;
import java.util.Random;
import java.util.Set;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.core.PartitionAwareKey;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

public class TestColocation {
	
	static HazelcastInstance instance;
	
	static IMap<Long, Customer> customers;
	static IMap<PartitionAwareKey,Order> orders;
	
	
	
	
	public TestColocation() {
		instance = Hazelcast.newHazelcastInstance();
		customers = instance.getMap("customer");
		orders = instance.getMap("orders");
	}
	
	private static class Customer implements Serializable{
		public long customerId;
		public String name;
		
		public Customer() {
			super();
			// TODO Auto-generated constructor stub
		}
		
		public Customer(long customerId, String name) {
			super();
			this.customerId = customerId;
			this.name = name;
		}
				
	}
	
	private static class Order implements Serializable {
		public long orderId;
		public long customerId;
		public long orderValue;
		public Order() {
			super();
			// TODO Auto-generated constructor stub
		}
		public Order(long orderId, long customerId, long orderValue) {
			super();
			this.orderId = orderId;
			this.customerId = customerId;
			this.orderValue = orderValue;
		}
		public long getOrderId() {
			return orderId;
		}
		public long getCustomerId() {
			return customerId;
		}
		public long getOrderValue() {
			return orderValue;
		}
		
		
		
		
		
	}
	
	private static class CustomerRunnable  implements Runnable ,Serializable{

		@Override
		public void run() {
			// Now I am processing all the customers
			// The question is if Customer and Order are co-located, how can I access the co located orders in a fast manour
			// Without excessive remote calls ?
			// I have a batch job and I want each node to process sequentialy all Customer and all Orders
			// I understand that it would be best to have one enchanced object
			// but due to limitations of the exiting prohect this might prove to be difficult
			Set<Long> localKeys = customers.localKeySet();
			for (long customerId:localKeys) {
				Predicate<Long, PartitionAwareKey> predicat = Predicates.equal("customerId", customerId);
				//this is slow, I believe there is remote call.
				orders.localKeySet(predicat);		
				
			}
			
		}	
		
	}
	
	public static class PartitionAwareKey implements PartitionAware<Long>,Serializable{
		
		public long customerId;
		private long key;
		
		
		public PartitionAwareKey() {
			super();
			// TODO Auto-generated constructor stub
		}
		
		public PartitionAwareKey(long customerId, long key) {
			super();
			this.customerId = customerId;
			this.key = key;
		}

		@Override
		public Long getPartitionKey() {
			// TODO Auto-generated method stub
			return customerId;
		}

		public long getCustomerId() {
			return customerId;
		}

		public long getKey() {
			return key;
		}
		
		
		
		
		
	}
		
	
	public void execute() {
		instance.getExecutorService("test").executeOnAllMembers(new CustomerRunnable());
	}
	
	public void populateTestData(int numberOfCustomers) {
		Random rand = new Random();
		int nextCustomerId=0;
		int nextOrderId=0;
		for (int i =0;i<numberOfCustomers;++i) {
			Customer customer = new Customer(++nextCustomerId,"John");
			customers.set(customer.customerId, customer);
			for (int k=0;k<rand.nextInt(3);++k) {
				Order order = new Order(++nextOrderId,customer.customerId,rand.nextInt(20000));
				orders.set(new PartitionAwareKey( order.customerId, order.orderId), order);
			}
		}
	}
	
	
	
	public static void main(String args[]) throws Exception{
		TestColocation test = new TestColocation();
		test.populateTestData(100);
		test.execute();
		Thread.currentThread().join();
		
	}

}
