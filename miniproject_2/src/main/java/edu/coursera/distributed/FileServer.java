package edu.coursera.distributed;

import java.net.ServerSocket;
import java.net.Socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {

    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs A proxy filesystem to serve files from. See the PCDPFilesystem
     * class for more detailed documentation of its usage.
     * @throws IOException If an I/O error is detected on the server. This
     * should be a fatal error, your file server implementation is not expected
     * to ever throw IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket, final PCDPFilesystem fs)
            throws IOException {
        /*
         * Enter a spin loop for handling client requests to the provided
         * ServerSocket object.
         */
        while (true) {
            Socket inputSocket = socket.accept();
            byte[] buffer = new byte[1024];
            int bytesReaded;
            String method;
            String path;
            String httpVersion;
            String requestStr;
            String[] parts;

            // TODO 1) Use socket.accept to get a Socket object
            /*
             * TODO 2) Using Socket.getInputStream(), parse the received HTTP
             * packet. In particular, we are interested in confirming this
             * message is a GET and parsing out the path to the file we are
             * GETing. Recall that for GET HTTP packets, the first line of the
             * received packet will look something like:
             *
             *     GET /path/to/file HTTP/1.1
             */
            InputStream inputStream = inputSocket.getInputStream();
            bytesReaded = inputStream.read(buffer);
            if (bytesReaded == buffer.length) {
                throw new RuntimeException("Consider to increase buffer size.");
            }
            StringBuilder sb = new StringBuilder(bytesReaded);
            for (int i = 0; i < bytesReaded; i++) {
                sb.append((char) buffer[i]);
            }
            requestStr = sb.toString();
            parts = requestStr.split("\\r\\n");
            String[] localParts = parts[0].split(" ");
            method = localParts[0];
            path = localParts[1];
            httpVersion = localParts[2];
            if (!"HTTP/1.1".equals(httpVersion)) {
                throw new RuntimeException("Only HTTP/1.1 is supported");
            }
            if (!"GET".equals(method)) {
                throw new RuntimeException("Only GET method allowed here");
            }

            /*
             * TODO 3) Using the parsed path to the target file, construct an
             * HTTP reply and write it to Socket.getOutputStream(). If the file
             * exists, the HTTP reply should be formatted as follows:
             *
             *   HTTP/1.0 200 OK\r\n
             *   Server: FileServer\r\n
             *   \r\n
             *   FILE CONTENTS HERE\r\n
             *
             * If the specified file does not exist, you should return a reply
             * with an error code 404 Not Found. This reply should be formatted
             * as:
             *
             *   HTTP/1.0 404 Not Found\r\n
             *   Server: FileServer\r\n
             *   \r\n
             *
             * Don't forget to close the output stream.
             */
            try (OutputStream outputStream = inputSocket.getOutputStream()) {
                StringBuilder responseBuilder = new StringBuilder(4096);
                String fileContent = fs.readFile(new PCDPPath(path));
                if (null == fileContent) {
                    responseBuilder.append("HTTP/1.0 404 Not Found\\r\\n");
                    responseBuilder.append("Server: FileServer\\r\\n");
                    responseBuilder.append("\\r\\n");
                    outputStream.write(responseBuilder.toString().getBytes());
                } else {
                    responseBuilder.append("HTTP/1.0 200 OK\\r\\n");
                    responseBuilder.append("Server: FileServer");
                    responseBuilder.append(System.lineSeparator());//donno why but it didn't work with "\\r\\n" 
                    responseBuilder.append(System.lineSeparator());//content just was as part of request headers
                    responseBuilder.append(fileContent);
                    //got also assertion error with crlf after content
                    outputStream.write(responseBuilder.toString().getBytes());
                }
            }

        }
    }
}
