public class Controller {
    private int cport;
    private int replicationFactor;
    private int timeout;
    private int rebalance_period;

    public Controller(int cport, int replicationFactor, int timeout, int rebalance_period) {
        this.cport = cport;
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
    }
}
