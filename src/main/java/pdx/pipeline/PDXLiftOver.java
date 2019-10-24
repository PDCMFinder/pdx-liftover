package pdx.pipeline;

import htsjdk.samtools.liftover.LiftOver;
import htsjdk.samtools.util.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

public class PDXLiftOver {

    private static final String ERRORSTR = "ERROR LIFTING";
    private static final long ERRORLONG = -1;
    private String chainFileURI;

    Logger log = LoggerFactory.getLogger(PDXLiftOver.class);

    private Map<String,long[]> errorFlag = new LinkedHashMap<>();


    Map<String,long[]> liftOverCoordinates(Map<String, long[]> genomeCoord) {

        errorFlag.put(ERRORSTR, new long[] {ERRORLONG, ERRORLONG} );

        return executeLiftOver(genomeCoord);
    }

    private Map<String,long[]> executeLiftOver(Map<String,long[]> genomeCoord) {

            LiftOver liftOver = new LiftOver(getChainFile());

            Interval inputInterval = mapToInterval(genomeCoord);
            Interval outputInterval;

            try {
                outputInterval = liftOver.liftOver(inputInterval);
            } catch(IllegalArgumentException e){
                outputInterval = null;
            }

            if(outputInterval == null) return errorFlag;
            else return intervalToMap(outputInterval);
    }

    private Map<String,long[]> intervalToMap(Interval interval){

        Map<String,long[]> map = new LinkedHashMap<>();
        String vcfFormatChromo = harmonizeChromoToVCF(interval.getSequence());
        map.put(vcfFormatChromo,
                new long[]{interval.getStart(), interval.getEnd()} );

        return map;
    }

    private Interval mapToInterval(Map<String,long[]> map) {
        Map.Entry<String, long[]> entry = map.entrySet().iterator().next();
        String harmonizedChromo = harmonizeChromoToChain(entry.getKey());
        return new Interval(harmonizedChromo, (int) entry.getValue()[0], (int) entry.getValue()[1]);
    }

    private String harmonizeChromoToVCF(String chromo){
        return chromo.toUpperCase().replaceAll("(?i)chr", "");
    }

    private String harmonizeChromoToChain(String chromo){

        String formatedChromo = chromo.trim();
        String completeExpression = "^.+";

        if(notChainFormat(chromo)) {
            if (matchesMitoCase(chromo))
                formatedChromo = chromo.replaceAll(completeExpression, "chrM");
            else
                formatedChromo = changeStringToChainFormat(chromo);
        }
        return formatedChromo;
    }

    private boolean matchesMitoCase(String chromo){
        return chromo.trim().matches("(?i)(chrmt|mt)");
    }

    private String changeStringToChainFormat(String chromo){
        return chromo.toUpperCase().replaceAll("(?i)([0-9]{1,2}|[xym])", "chr$1");
    }

    private boolean notChainFormat(String chromo){
        return ! chromo.matches("chr([0-9]{1,2}|[XYM])");
    }

    public File getChainFile(){
        return new File(chainFileURI);
    }

    public void setChainFileURI(String chainFileURI){
        this.chainFileURI = chainFileURI;
    }

}