package streaming;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
		Config config = new Config();
		config.setDebug(false);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("yahoo-finance-spout-msft", new YahooFinanceSpout("MSFT"));
		builder.setSpout("yahoo-finance-spout-googl", new YahooFinanceSpout("GOOGL"));
		builder.setSpout("yahoo-finance-spout-aapl", new YahooFinanceSpout("AAPL"));
		builder.setBolt("price-cutoff-bolt", new YahooFinanceUsain()).
			fieldsGrouping("yahoo-finance-spout-msft", new Fields("company")).
			fieldsGrouping("yahoo-finance-spout-googl", new Fields("company")).
			fieldsGrouping("yahoo-finance-spout-aapl", new Fields("company"));
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("YahooFinanceStorm", config, builder.createTopology());
		Thread.sleep(30000);
		cluster.shutdown();
	}
}