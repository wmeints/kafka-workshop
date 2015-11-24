package nl.infosupport.kafkaworkshop.fanout;


@FunctionalInterface
public interface MessageAction {
    void execute(String messageContent);
}
