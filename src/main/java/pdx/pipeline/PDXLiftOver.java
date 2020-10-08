package pdx.pipeline;

import htsjdk.samtools.liftover.LiftOver;
import htsjdk.samtools.util.Interval;

import java.io.File;
import java.util.*;

public class PDXLiftOver {

    private static final String ERRORSTR = "ERROR LIFTING";
    private String chainFileURI;

    public List<String> liftOverCoordinates(Map<String, long[]> genomeCoord) {
        return executeLiftOver(genomeCoord);
    }

    private List<String> executeLiftOver(Map<String,long[]> genomeCoord) {
        LiftOver liftOver = new LiftOver(getChainFile());
        List<String> allIntervals = new ArrayList<>();
        genomeCoord.entrySet().forEach(entry  -> {
          Interval inputInterval = entryToInterval(entry);
            Interval outputInterval;
            try {
                outputInterval = liftOver.liftOver(inputInterval);
            } catch(IllegalArgumentException e){
                outputInterval = null;
            }
            if(outputInterval == null) {
                allIntervals.add(ERRORSTR);
            }
            else {
                allIntervals.addAll(intervalToList(outputInterval));
            }
        });
        return allIntervals;
    }
    private List<String> intervalToList(Interval interval){
        List<String> list = new LinkedList<>();
        String vcfFormatChromo = harmonizeChromoToVCF(interval.getSequence());
        list.add(vcfFormatChromo);
        list.add(String.valueOf(interval.getStart()));
        list.add(String.valueOf(interval.getEnd()));
        return list;
    }

    private Interval entryToInterval(Map.Entry<String,long[]> entry) {
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
            if (isNumericSexChromo(chromo)){
                formatedChromo = formatNumericSexChromoToChain(chromo);
            }
            else
                formatedChromo = changeStringToChainFormat(chromo);
        }
        return formatedChromo;
    }

    private String formatNumericSexChromoToChain(String chromo){
        String formatedChromo;
        if (chromo.matches("^.*23")){
            formatedChromo = "chrX";
        }
        else {
            formatedChromo = "chrY";
        }
        return formatedChromo;
    }

    private boolean matchesMitoCase(String chromo){
        return chromo.trim().matches("(?i)(chrmt|mt)");
    }

    private String changeStringToChainFormat(String chromo){
        return chromo.toUpperCase().replaceAll("(?i)^(?:chr)?([0-9]{1,2}|[xym]|Un).*$", "chr$1");
    }

    private boolean isNumericSexChromo(String chromo){
        return chromo.matches("^.*2[34]");

    }

    private boolean notChainFormat(String chromo){
        return ! chromo.matches("(?i)^chr([0-9]{1,2}|[XYM]|Un)$");
    }

    public File getChainFile(){
        return new File(chainFileURI);
    }

    public void setChainFileURI(String chainFileURI){
        this.chainFileURI = chainFileURI;
    }
}