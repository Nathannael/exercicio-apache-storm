package streaming;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

public class YahooFinanceSpout implements IRichSpout {
	private SpoutOutputCollector collector;
	private String symbol;
	
	public YahooFinanceSpout(String symbol) {
		this.symbol = symbol;
	}

	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	public void activate() {
		// TODO Auto-generated method stub

	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public void deactivate() {
		// TODO Auto-generated method stub

	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	public void nextTuple() {
		DateTimeFormatter formatter = DateTimeFormatter.BASIC_ISO_DATE;
		try {
		StockQuote quote = YahooFinance.get(symbol).getQuote(); // Financas da Microsoft
		BigDecimal price = quote.getPrice();
		BigDecimal prevClose = quote.getPreviousClose();
		collector.emit(new Values(symbol, formatter.format(LocalDate.now()), price.doubleValue(),
		prevClose.doubleValue()));
		} catch (IOException e) {
		e.printStackTrace();
		}
		}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("company", "timestamp", "price", "prev_close"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
