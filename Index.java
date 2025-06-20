import java.util.*;

public class Index {

    public enum FileStatus {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE,
    }

    private HashMap<String, FileInformation> fileIndex;

    public Index() {
        this.fileIndex = new HashMap<String, FileInformation>();
    }

    public static class FileInformation{
        private FileStatus status;
        private long fileSize;
        private ArrayList<Integer> storagePorts; //Ports of the Dstores that contain the file

        public FileInformation (FileStatus status, long fileSize, ArrayList<Integer> storagePorts) {
            this.status = status;
            this.fileSize = fileSize;
            this.storagePorts = storagePorts;
        }

        public FileStatus getStatus() {
            return status;
        }

        public void setStatus(FileStatus status) {
            this.status = status;
        }

        public long getFileSize() {
            return fileSize;
        }

        public void setFileSize(long fileSize) {
            this.fileSize = fileSize;
        }

        public ArrayList<Integer> getStoragePorts() {
            return storagePorts;
        }

        public void setStoragePorts(ArrayList<Integer> storagePorts) {
            this.storagePorts = storagePorts;
        }
    }

    public synchronized void addFile(String fileName, long fileSize, ArrayList<Integer> storagePorts) { // Use of keyword synchronized to ensure no concurrent access
        FileInformation metadata = new FileInformation(FileStatus.STORE_IN_PROGRESS, fileSize, storagePorts);
        fileIndex.put(fileName, metadata);
    }

    public synchronized void removeFile(String fileName) {
        fileIndex.remove(fileName);
    }

    public synchronized void updateFileStatus(String fileName, FileStatus status) {
        FileInformation metadata = fileIndex.get(fileName);
        if (metadata != null) {
            metadata.setStatus(status);
        }else{
            System.err.println("ERROR: No metadata stored for this file");
        }
    }

    public synchronized FileInformation getFileInformation(String fileName) {
        return fileIndex.get(fileName);
    }

    public synchronized ArrayList<String> getFileList() {
        ArrayList<String> fileList = new ArrayList<String>();
        for (Map.Entry<String, FileInformation> entry : fileIndex.entrySet()) {
            if (entry.getValue().getStatus() == FileStatus.STORE_COMPLETE) { // Only show files that are stored, this also helps to stop concurrent operations being accessed
                fileList.add(entry.getKey());
            }
        }
        return fileList;

    }

    public HashMap<String, FileInformation> getFileIndex() {
        return fileIndex;
    }

}
