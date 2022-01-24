package pdx.pipeline;

import me.tongfei.progressbar.ProgressBar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class OmicHarmonizer {

    public enum OMIC {
        MUT,
        CNA,
        DEFAULT
    }

    private OMIC omicType;
    private ArrayList<ArrayList<String>> omicSheet;
    private ArrayList<ArrayList<String>> outputSheet;
    private int chromosomeColumn;
    private int seqStartPositionCol = -1;
    private int seqEndPositionCol = -1;

    private static final String CHROMOSOME = "chromosome";
    private static final String SEQSTARTPOS = "seq_start_position";
    private static final String ERRORSTR = "ERROR LIFTING";

    private final PDXLiftOver lifter = new PDXLiftOver();

    Logger log = LoggerFactory.getLogger(OmicHarmonizer.class);
    Path logFileLocation;

    public OmicHarmonizer(String chain) {
        lifter.setChainFileURI(chain);
    }

    public ArrayList<ArrayList<String>> runLiftOver(ArrayList<ArrayList<String>> sheet, String fileURI, OMIC dataType) throws IOException {
        setOmicType(dataType);
        if(sheet.size() > 0) {
            setClassVariables(sheet, fileURI);
            initHeaders();
            buildLiftOverResults(fileURI);
        } else log.error(String.format("File appears to be empty %s", fileURI));
            return outputSheet;
    }

    private void setClassVariables(ArrayList<ArrayList<String>> sheet, String fileURI){
        omicSheet = sheet;
        Path parentPath = Paths.get(fileURI).getParent();
        logFileLocation = Paths.get(parentPath.toString() + "/lift.log");
        outputSheet = new ArrayList<>();
    }

    private void initHeaders() {
        chromosomeColumn = getColumnByHeader(CHROMOSOME);
        seqStartPositionCol = getColumnByHeader(SEQSTARTPOS);
    }

    private void buildLiftOverResults(String fileURI){
        if (headersAreNotMissing()) {
            outputSheet.add(getHeaders());
            log.info(String.format("Lifting file %s", fileURI));
            iterateThruLiftOver();
        } else log.error(String.format("Headers not found on file %s", fileURI));
    }

    private void iterateThruLiftOver(){
        ProgressBar pb = new ProgressBar("Lifting", omicSheet.size()).start();
        int count = 0;
        for (ArrayList<String> row : omicSheet) {
            if (omicSheet.indexOf(row) != 0) {
                liftOver(row);
            }
            if((count % 100) == 0) {
                pb.stepBy(100);
            }
            count++;
        }
        pb.stop();
    }

   private void liftOver(ArrayList<String> row){
       List<String> liftedData = lifter.liftOverCoordinates(getRowsGenomicCoordinates(row));
       if ((liftedData.isEmpty() || liftedData.contains(ERRORSTR) || liftedData.contains("-1"))) {
           String infoMsg = String.format("LiftOver: Genomic coordinates not lifted for row at index: %s. %n Row data : %s", omicSheet.indexOf(row), Arrays.toString(row.toArray()));
           logLiftInfo(infoMsg);
       } else {
           harmonizeData(liftedData, row);
       }
   }

    private void logLiftInfo(String message){
        log.info(message);
        try {
            if(logFileLocation.toFile().exists()) {
                Files.write(logFileLocation, Collections.singleton(message), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            }
            else {
                Files.write(logFileLocation, Collections.singleton(message), StandardCharsets.UTF_8, StandardOpenOption.CREATE);
            }
        } catch (IOException e) {
            log.info(String.format("IOError writing to log file %s", logFileLocation));
            e.printStackTrace();
        }
    }

    private void harmonizeData(List<String> liftedData, ArrayList<String> row){
        ArrayList<String> rowOut = new ArrayList<>(row);
        mergeLiftDataWithRowData(liftedData, rowOut);
        outputSheet.add(rowOut);
    }

    private Map<String, long[]> getRowsGenomicCoordinates(ArrayList<String> row){
        String rowChromosome = "";
        long rowStartPos = -1;
        long endPos = -1;
        if(row.size() > chromosomeColumn && row.size() > seqStartPositionCol && row.size() > seqEndPositionCol) {
                rowChromosome = row.get(chromosomeColumn);
                rowStartPos = getAndValidateSeqCoordinatesNum(row, seqStartPositionCol);
                endPos = getSeqEndPosition(row);
                if(rowChromosome.equals("")) log.info("No Chromosome information found for index " + omicSheet.indexOf(row));
                if(rowStartPos == -1 || endPos == -1) log.info("Start or end pos missing in " + omicSheet.indexOf(row));
        } else log.error("Error column size is less then header at index: " + omicSheet.indexOf(row));
        Map<String, long[]> genomCoors = new LinkedHashMap<>();
        genomCoors.put(rowChromosome, new long[]{rowStartPos, endPos});
        return genomCoors;
    }

    private long getAndValidateSeqCoordinatesNum(ArrayList<String> row, int colNum){
        return Long.parseLong(validateNumStr(row.get(colNum)));
    }

    private String validateNumStr(String num){
        return num.trim().equals("") ? "-1" : num;
    }

    private long getSeqEndPosition(ArrayList<String> row) {
        long endPos = -1;
        if(omicType.equals(OMIC.CNA)) endPos = getAndValidateSeqCoordinatesNum(row, seqEndPositionCol);
        else if(omicType.equals(OMIC.MUT)) endPos = getAndValidateSeqCoordinatesNum(row, seqStartPositionCol);
        return endPos;
    }

    private void mergeLiftDataWithRowData(List<String> liftedData, ArrayList<String>row) {
        row.set(chromosomeColumn, liftedData.get(0));
        row.set(seqStartPositionCol, liftedData.get(1));
    }

    public int getColumnByHeader(String header) {
        ArrayList<String> headers = getHeaders();
        Iterator<String> iterator = headers.iterator();
        int errorFlag = -1;
        boolean foundMatch;
        int index = -1;
        do {
            index++;
            foundMatch = iterator.next().equalsIgnoreCase(header);
        }while(iterator.hasNext() && ! foundMatch);
        if(foundMatch)
            return index;
        else
            return errorFlag;
    }

    protected ArrayList<String> getHeaders(){
        return omicSheet.get(0);
    }

    protected boolean headersAreNotMissing(){
        return (chromosomeColumn != -1 && seqStartPositionCol != -1);
    }

    public void setOmicSheet(ArrayList<ArrayList<String>> omicSheet) {
        this.omicSheet = omicSheet;
    }

    public void setOmicType(OMIC omicType){
        this.omicType = omicType;
    }

    public OMIC getOmicType() {
        return omicType;
    }

}
