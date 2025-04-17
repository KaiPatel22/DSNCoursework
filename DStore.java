public class DStore {

    private int port;
    private int cport;
    private int timeout;
    private String file_folder;

    public DStore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];

        DStore dstore = new DStore(port, cport, timeout, file_folder);
    }
}
