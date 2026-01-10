import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DFSServer {
    private static final int PORT = 8080;
    // ファイル内容をメモリ上で管理
    private static Map<String, File> fileStore = new ConcurrentHashMap<>();
    // 書き込み制限(同時書き込みを防ぐ)
    private static Map<String, String> writeLocks = new ConcurrentHashMap<>();

    private static enum modeSet { //アクセスモードの集合
        READ_ONLY, WRITE_ONLY, RW
    }

    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("DFS Server started on port " + PORT);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(new ClientHandler(clientSocket)).start();
        }
    }

    static class File{// ファイル情報を管理するクラス
        String content; // ファイル内容
        LocalDateTime lastsaved; // 最終保存日時(closeにより更新された時間)
        LocalDateTime lastaccessed; // 最終アクセス日時(openした時間)

        public File() {
            this.content = "";
            this.lastsaved = null;
            this.lastaccessed = LocalDateTime.now();
        }
    }

    private static class ClientHandler implements Runnable {
        private Socket socket;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                String line = in.readLine();
                if (line == null)
                    return;
                String[] parts = line.split(" ", 3);
                String command = parts[0]; //FETCH or STORE

                if ("FETCH".equals(command)) {
                    String path = parts[1];
                    String mode = parts[2];

                    while(true){//期待されるmodeでなければerrorを返す
                        try {
                            modeSet.valueOf(mode);
                            break;
                        } catch(IllegalArgumentException e){
                            out.println("ERROR: Invalid mode. mode should be READ_ONLY, WRITE_ONLY, or RW");
                            mode = in.readLine();
                        }
                    }


                    // 書き込みモード -> ロック管理
                    if (mode.contains("WRITE")) {
                        if (writeLocks.containsKey(path)) {
                            out.println("ERROR: File is locked by another client");
                            return;
                        }
                        writeLocks.put(path, socket.getRemoteSocketAddress().toString());
                    }

                    File filedata = fileStore.get(path);
                    if(filedata == null){
                        filedata = new File();
                        filedata.lastaccessed = LocalDateTime.now();
                        fileStore.put(path, filedata);// ファイル内容を保存
                    }else{
                        filedata.lastaccessed = LocalDateTime.now();
                    }
                    
                    out.println("SUCCESS");
                    out.println(filedata.content); // 実際は終端文字などの工夫が必要

                } else if ("STORE".equals(command)) {
                    String path = parts[1];
                    StringBuilder content = new StringBuilder();
                    String inputLine;

                    while (!(inputLine = in.readLine()).equals("__END__")) {
                        content.append(inputLine).append("\n");
                    }
                    File filedata = fileStore.get(path);
                    if (filedata == null) {
                        System.out.println("Creating new file entry for: " + path);
                        filedata = new File();
                    }

                    filedata.content = content.toString().trim();// ファイル内容
                    filedata.lastsaved = LocalDateTime.now(); // 最終保存日時
                     
                    fileStore.put(path, filedata);// ファイル内容を保存
                    writeLocks.remove(path); // 更新後にロック解除
                    out.println("SUCCESS");

                } else if("UNLOCK".equals(command)) {
                    String path = parts[1];
                    writeLocks.remove(path); // ロックを解除
                    out.println("SUCCESS");

                } else if ("LIST_FILES".equals(command)) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                   
                    out.println("FILES_LIST_START");//
                    out.println("File List:");
                    out.println("Filename \t Last Saved \t\t Last Accessed");

                    for (String filePath : fileStore.keySet()) {
                        out.print("- " + filePath);//ファイル名を送信

                        if(fileStore.get(filePath).lastsaved != null){//最終保存日時があれば送信
                            out.print(" \t " + fileStore.get(filePath).lastsaved.format(formatter));
                        }else{
                            out.print(" \t " + "***");
                        }

                        if(fileStore.get(filePath).lastaccessed != null){//最終アクセス日時があれば送信
                            out.println(" \t " + fileStore.get(filePath).lastaccessed.format(formatter));
                        }else{
                            out.println(" \t " + "***");
                        }

                    }
                    out.println("FILES_LIST_END");

                } else if("VIEW_LOCKS".equals(command)) {
                    out.println("LOCKS_LIST_START");
                    for (Map.Entry<String, String> entry : writeLocks.entrySet()) {
                        out.println("File: " + entry.getKey() + " locked by " + entry.getValue());
                    }
                    out.println("LOCKS_LIST_END");
                
                }else {
                    out.println("ERROR: Unknown command");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
