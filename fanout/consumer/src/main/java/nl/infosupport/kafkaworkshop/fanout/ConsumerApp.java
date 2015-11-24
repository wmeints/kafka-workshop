package nl.infosupport.kafkaworkshop.fanout;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

public class ConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);

    public static void main(String[] args) throws Exception {

        Options opts = new Options();
        opts.addOption(Option.builder("zookeeper").desc("Zookeeper host to connect to").hasArg().required().build());
        opts.addOption(Option.builder("topic").desc("Topic to consume from").hasArg().required().build());
        opts.addOption(Option.builder("group").desc("Group identifier for the consumer").hasArg().required().build());

        CommandLineParser parser = new DefaultParser();

        try {

            CommandLine cmd = parser.parse(opts, args);

            kafka.javaapi.consumer.ConsumerConnector consumer = ConsumerConnectorFactory.createConsumer(
                    cmd.getOptionValue("zookeeper"), cmd.getOptionValue("group"));

            MessageListener listener = new MessageListener(consumer);

            // Start to listen on the specified topic.
            // You can start the listener just once in this demo.
            listener.start(cmd.getOptionValue("topic"), 1, msg -> {
                log.info("Received message: {}", msg);
            });

            // The rest of this method is a nasty trick to prevent Java from shutting down on us.
            // Please ignore this and don't use if you have better ways to control the runtime.
            Semaphore signal = new Semaphore(0, false);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    signal.release();
                }
            });

            signal.acquire();
        }
        catch(ParseException ex) {
            System.out.println(ex.getMessage());

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("fanout-consumer", opts);
        }
    }


}
