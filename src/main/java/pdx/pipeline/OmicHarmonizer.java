package pdx.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class OmicHarmonizer {

    enum OMIC {
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
    private int genomeAssemblyCol = -1;

    private static final String CHROMOSOME = "chromosome";
    private static final String SEQSTARTPOS = "seq_start_position";
    private static final String SEQENDPOS = "seq_end_position";
    private static final String GENOMEASSEMBLY = "genome_assembly";
    private static final String ERRORSTR = "ERROR LIFTING";
    private static final int BROKEN_ROW_THRESHOLD = 25;

    private static int brokenRowCount = 0;

    private PDXLiftOver lifter = new PDXLiftOver();

    Logger log = LoggerFactory.getLogger(OmicHarmonizer.class);
    String workingFile;
    Path logFileLocation;

    OmicHarmonizer(String chain) {
        lifter.setChainFileURI(chain);
    }

    protected ArrayList<ArrayList<String>> runLiftOver(ArrayList<ArrayList<String>> sheet, String fileURI, OMIC dataType) throws IOException {

        setOmicType(dataType);
        omicSheet = sheet;
        workingFile = fileURI;
        Path parentPath = Paths.get(fileURI).getParent();
        logFileLocation = Paths.get(parentPath.toString() + "/data.log");

        outputSheet = new ArrayList<>();
        initHeaders();

        if(headersAreNotMissing()) {
            outputSheet.add(getHeaders());
            iterateThruLiftOver();
        }
        else throw new IOException("Headers are not found on file" );
        return outputSheet;
    }

    private void initHeaders() {

        findLastHeaderColumn();
        chromosomeColumn = getColumnByHeader(CHROMOSOME);
        seqStartPositionCol = getColumnByHeader(SEQSTARTPOS);

        genomeAssemblyCol = getColumnByHeader(GENOMEASSEMBLY);
        if (omicType.equals(OMIC.CNA)) seqEndPositionCol = getColumnByHeader(SEQENDPOS);
    }

    private void findLastHeaderColumn(){

        int endCol = getHeaders().size();

        for(int i = 0; i < endCol; i++) {
            if (getHeaders().get(i).trim().equals("")) endCol = i;
        }
    }

    private void iterateThruLiftOver() throws IOException {
        for (ArrayList<String> row : omicSheet) {

            if (omicSheet.indexOf(row) == 0) continue;

            if (hasGenomeAssemblyColAndisHg37(row)) {

                Map<String, long[]> liftedData = lifter.liftOverCoordinates(getRowsGenomicCoor(row));
                if ((liftedData.isEmpty() || liftedData.containsKey(ERRORSTR) || liftedData.containsValue(-1))) {
                    String infoMsg = String.format("LiftOver: Genomic coordinates not lifted for row at index: %s. %n Row data : %s", omicSheet.indexOf(row), Arrays.toString(row.toArray()));
                    logLiftInfo(row, infoMsg);

                }
                else {
                    harmonizeData(liftedData, row);
                }
            }
            else if (hasGenomeAssemblyColAndisHg38(row)) {
                outputSheet.add(row);
            }
            else {
                String mgs = String.format("LiftOver: row does not have Genome Assemlby colum. Index : %s Row data: %s", omicSheet.indexOf(row), Arrays.toString(row.toArray()));
                logLiftInfo(row, mgs);
                if (brokenThresholdIsMet()) {
                    String alterMSG = String.format("LiftOver: **********************BROKEN COLUMN THRESHOLD MET IN %s********************** \n **********************Stopped reading file**********************", workingFile);
                    logLiftInfo(row,alterMSG);
                    break;
                }
            }
        }
        brokenRowCount = 0;
    }

    protected boolean brokenThresholdIsMet() {
        return ((++brokenRowCount) >= BROKEN_ROW_THRESHOLD);
    }

    protected boolean hasGenomeAssemblyColAndisHg37(ArrayList<String> row){
        return row.size() > genomeAssemblyCol && row.get(genomeAssemblyCol).trim().matches("(?i)(37|19|Hg19|GRCh37)");
    }

    protected boolean hasGenomeAssemblyColAndisHg38(ArrayList<String> row){
        return row.size() > genomeAssemblyCol && row.get(genomeAssemblyCol).trim().matches("(?i)(38|hg38|grch38)");
    }

    private void logLiftInfo(ArrayList<String> row, String message) throws IOException {
        log.info(message);
        if(logFileLocation.toFile().exists()) {
            Files.write(logFileLocation, Collections.singleton(message), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        }
        else {
            Files.write(logFileLocation, Collections.singleton(message), StandardCharsets.UTF_8, StandardOpenOption.CREATE);
        }
        }

    private void harmonizeData(Map<String,long[]> liftedData, ArrayList<String> row){

        ArrayList<String> rowOut = new ArrayList<>(row);
        mergeLiftDataWithRowData(liftedData, rowOut);
        updateAssembly(rowOut);
        outputSheet.add(rowOut);
    }

    private Map<String, long[]> getRowsGenomicCoor(ArrayList<String> row){

        String rowChromosome = "";
        long rowStartPos = -1;
        long endPos = -1;

        if(row.size() > chromosomeColumn && row.size() > seqStartPositionCol && row.size() > seqEndPositionCol) {
             rowChromosome = row.get(chromosomeColumn);
             rowStartPos = getAndValidateSeqCoorNum(row, seqStartPositionCol);
             endPos = getSeqEndPosition(row);
        } else log.error("Error column size is less then header at index: " + omicSheet.indexOf(row));

        if(rowChromosome.equals("")) log.info("No Chromsome information found for index " + omicSheet.indexOf(row));
        if(rowStartPos == -1 || endPos == -1) log.info("Start or end pos missing in " + omicSheet.indexOf(row));

         Map<String, long[]> genomCoors = new LinkedHashMap<>();
         genomCoors.put(rowChromosome, new long[] { rowStartPos, endPos});

         return genomCoors;
    }

    private long getAndValidateSeqCoorNum(ArrayList<String> row, int colNum){
        return Long.parseLong(validateNumStr(row.get(colNum)));
    }

    private String validateNumStr(String num){
        return num.trim().equals("") ? "-1" : num;
    }

    private long getSeqEndPosition(ArrayList<String> row) {

        long endPos = -1;
        if(omicType.equals(OMIC.CNA)) endPos = getAndValidateSeqCoorNum(row, seqEndPositionCol);
        else if(omicType.equals(OMIC.MUT)) endPos = getAndValidateSeqCoorNum(row, seqStartPositionCol);
        return endPos;
    }

    private void setSeqEndPos(ArrayList<String> row, String endPos){
        if (omicType.equals(OMIC.CNA)) row.set(seqEndPositionCol, endPos);
    }

    private void mergeLiftDataWithRowData(Map<String,long[]> liftedData, ArrayList<String>row) {

        Set<Map.Entry<String,long[]>> set = liftedData.entrySet();
        ArrayList<Map.Entry<String,long[]>> list = new ArrayList<>(set);

        for(Map.Entry<String,long[]> entry : list ){

            row.set(chromosomeColumn,entry.getKey());
            row.set(seqStartPositionCol,String.valueOf(entry.getValue()[0]));

            setSeqEndPos(row, String.valueOf(entry.getValue()[1]));
        }
    }

    protected void updateAssembly(ArrayList<String> row){
        row.set(genomeAssemblyCol, "GRCh38");
    }

    protected int getColumnByHeader(String header) {

        ArrayList<String> headers = getHeaders();
        Iterator<String> iterator = headers.iterator();
        int errorFlag = -1;

        boolean foundMatch = false;
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
        return (chromosomeColumn != -1 && genomeAssemblyCol != -1 &&  seqStartPositionCol != -1);
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
