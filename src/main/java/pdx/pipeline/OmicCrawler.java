package pdx.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static pdx.pipeline.OmicHarmonizer.*;

public class OmicCrawler {

    ArrayList<File> providersData;
    ArrayList<File> variantData = new ArrayList<>();

    private OMIC crawlerSetting = OMIC.DEFAULT;

    Logger log = LoggerFactory.getLogger(OmicCrawler.class);

    public List<File> run(File UPDOG) throws IOException {
        return searchFileTreeForOmicData(UPDOG);
    }

    public List<File> searchFileTreeForOmicData(File rootDir) throws IOException {
        if(folderExists(rootDir)) {
            String updog = String.format("%s/data/UPDOG", rootDir);
            File whatUpDog = new File(updog);
            List<File> providerFolders;

            if (whatUpDog.exists()) {
                providerFolders = Arrays.asList(whatUpDog.listFiles());
            } else {
                System.out.println("No Updog found. Default to folder search mode");
                providerFolders = new ArrayList<>();
                providerFolders.add(rootDir);
            }
            providersData = returnMutAndCNASubFolders(providerFolders);
            variantData = getVariantdata(providersData);
        } else throw new IOException("Error root directory could not be found by the OmicCrawler");
        return variantData;
    }

    private ArrayList<File> returnMutAndCNASubFolders(List<File> rootDir){
        ArrayList<File> providers = new ArrayList<>();
        rootDir.forEach(f ->
                {
                    if(f.exists() && f.isDirectory()
                    ) {
                    providers.addAll
                            (Arrays.stream(f.listFiles())
                                    .filter(t -> t.getName().matches(getFileFilter()))
                                    .collect(Collectors.toCollection(ArrayList::new)));
                }
                    }
        );
        return providers;
    }

    private String getFileFilter() {
        String regexFilter = "";
        switch(crawlerSetting)
        {
            case CNA :
                regexFilter = "(?i)CNA";
                break;
            case MUT :
                regexFilter = "(?i)MUT";
                break;
            default:
                regexFilter = "(?i)(MUT|CNA)";
        }
        return regexFilter;
    }

    private ArrayList<File> getVariantdata(ArrayList<File> providersData) {
        if( ! providersData.isEmpty()) {
            return returnMutAndCNAFiles(providersData);
        }
        else {
            return new ArrayList<>();
        }
    }

    private ArrayList<File> returnMutAndCNAFiles(List<File> providersData) {
        providersData.forEach(f ->
            variantData.addAll
                    (Arrays.stream(f.listFiles())
                            .filter(t -> t.getName().matches(".+(xlsx|tsv|csv)"))
                            .collect(Collectors.toCollection(ArrayList::new)))
        );
        return variantData;
    }

    private boolean folderExists(File rootDir){
        return rootDir.exists();
    }

    public OMIC getCrawlerSetting() {
        return crawlerSetting;
    }

    public void setCrawlerSetting(OMIC crawlerSetting) {
        this.crawlerSetting = crawlerSetting;
    }
}
