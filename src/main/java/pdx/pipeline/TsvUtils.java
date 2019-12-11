package pdx.pipeline;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsvUtils {


    public void writeTSV(ArrayList<ArrayList<String>> sheet, String fileURI) throws IOException {

        FileWriter tsvWriter = new FileWriter(fileURI);

        for(ArrayList<String> rowData : sheet) {

            tsvWriter.append((String.join("\t", rowData)));
            tsvWriter.append("\n");

        }
        tsvWriter.flush();
        tsvWriter.close();
    }

    public ArrayList<ArrayList<String>> readCsv(String dataFile, String delimiter){

        FileInputStream fileStream = null;
            try{
            fileStream = new FileInputStream(dataFile);
        }catch (Exception e){
            e.printStackTrace();
        }
        DataInputStream myInput = new DataInputStream(fileStream);


        String thisLine;
        int i=0;
        ArrayList lineList = null;
        ArrayList<ArrayList<String>> dataArrayList = new ArrayList<>();

            try {

            while ((thisLine = myInput.readLine()) != null)
            {
                lineList = new ArrayList();

                String strar[] = thisLine.split(delimiter);
                for(int j=0;j<strar.length;j++)
                {
                    lineList.add(strar[j]);
                }
                dataArrayList.add(lineList);
                i++;
            }

        }catch (Exception e){
            e.printStackTrace();
        }

            return dataArrayList;
    }

}
