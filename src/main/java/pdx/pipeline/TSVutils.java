package pdx.pipeline;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class TSVutils {


    public void writeTSV(ArrayList<ArrayList<String>> sheet, String fileURI) throws IOException {

        FileWriter tsvWriter = new FileWriter(fileURI);

        for(ArrayList<String> rowData : sheet) {

            tsvWriter.append((String.join("\t", rowData)));
            tsvWriter.append("\n");

        }
        tsvWriter.flush();
        tsvWriter.close();
    }
}
