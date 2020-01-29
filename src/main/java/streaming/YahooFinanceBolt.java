package streaming;

import java.io.PrintWriter;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class YahooFinanceBolt implements IRichBolt {
	private OutputCollector collector;
	private PrintWriter writer;

	public void cleanup() {
		writer.close();
		}

	public void execute(Tuple input) {
		String symbol = input.getValue(0).toString();
		String timestamp = input.getString(1);
		Double price = (Double) input.getValueByField("price");
		Double preClose = (Double) input.getValueByField("prev_close");
		Boolean gain = price>preClose;
		this.writer.println(symbol + "," + timestamp + "," + price + "," + gain + "," + preClose);
		collector.emit(new Values(symbol, timestamp, price, gain));
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		String filename = "/tmp/YahooFinance.txt";
		this.collector = collector;
		try {
		this.writer = new PrintWriter(filename, "UTF-8");
		} catch (Exception e) {
		throw new RuntimeException("Erro ao abrir o arquivo ["+filename+"]");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("company", "timestamp", "price", "gain"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
