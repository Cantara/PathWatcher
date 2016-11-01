package no.cantara.file.util;

import org.w3c.dom.Node;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.stream.Collectors;

/**
 * Created by oranheim on 28/09/2016.
 */
public class CommonUtil {

    private static ThreadLocal<OutputStream> outputLocal = new ThreadLocal<OutputStream>() {
        private OutputStream output = null;

        @Override
        protected OutputStream initialValue() {
            if (output == null) {
                output = newOutputStream();
            }
            return output;
        }

        @Override
        public void remove() {
            try {
                output.flush();
                output.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            super.remove();
        }
    };

    public static void closeOutputStream(OutputStream output) throws IOException {
        output.flush();
        output.close();
    }

    public static OutputStream closeAndCreateNewOutputStream(OutputStream output) throws IOException {
        closeOutputStream(output);
        return newOutputStream();
    }

    public static OutputStream getConsoleOutputStream() {
        return outputLocal.get();
    }

    public static OutputStream newOutputStream() {
        return new OutputStream() {
            private StringBuilder string = new StringBuilder();

            @Override
            public void write(int b) throws IOException {
                this.string.append((char) b);
            }

            @Override
            public synchronized void write(byte[] b, int off, int len) {
                try {
                    this.string.append(new String(b, 0, len, "UTF-8"));
                } catch (Exception e) {

                }
            }


            public String toString() {
                return this.string.toString();
            }
        };
    }

    public static OutputStream writeInputToOutputStream(InputStream in) throws IOException {
        OutputStream out = newOutputStream();
        byte[] buffer = new byte[1024];
        int len = in.read(buffer);
        while (len != -1) {
            out.write(buffer, 0, len);
            len = in.read(buffer);
        }
        out.close();
        return out;
    }

    public static String domElementToXmlString(Node node) throws TransformerException {
        TransformerFactory tff = TransformerFactory.newInstance();
        Transformer tf = tff.newTransformer();

        tf.setOutputProperty(OutputKeys.METHOD, "xml");
        tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        tf.setOutputProperty(OutputKeys.INDENT, "yes");
        tf.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "3");

        Source sc = new DOMSource(node);

        ByteArrayOutputStream streamOut = new ByteArrayOutputStream();
        StreamResult result = new StreamResult(streamOut);
        tf.transform(sc, result);

        return streamOut.toString();
    }

    public static String getActualSoruceAsSource(ClassLoader classLoader, String rn) throws Exception {
        return new BufferedReader(new InputStreamReader(classLoader
                .getResourceAsStream(rn)))
                .lines().collect(Collectors.joining("\n"));
    }

}
