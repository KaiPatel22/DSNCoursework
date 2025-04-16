public class DStore {
    int port;
    int cport;
    int timeout;
    String file_folder;

    public DStore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
    }
}
