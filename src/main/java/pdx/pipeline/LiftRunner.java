package pdx.pipeline;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Component
public class LiftRunner implements CommandLineRunner {

    @Value("${pdxfinder.root.dir}")
    private String finderRootDir;

    private Logger log = LoggerFactory.getLogger(LiftRunner.class);

    private OmicCrawler crawler = new OmicCrawler();
    private XlsxReader reader = new XlsxReader();
    private OmicHarmonizer harmonizer = new OmicHarmonizer(CHAINFILE);
    private TsvUtils tsvUtil = new TsvUtils();

    private static final String CHAINFILE = "src/main/resources/LiftOverResources/hg19ToHg38.over.chain.gz";

    @Override
    public void run(String... args) throws Exception {

        OptionParser parser = new OptionParser();
        parser.allowsUnrecognizedOptions();
        parser.accepts("LIFT");
        parser.accepts("MUT");
        parser.accepts("CNA");
        parser.accepts("DIR").withRequiredArg();
        OptionSet options = parser.parse(args);

        if (options.has("DIR")) finderRootDir = (String) options.valueOf("DIR");
        if (options.has("CNA")) crawler.setCrawlerSetting(OmicHarmonizer.OMIC.CNA);
        if (options.has("MUT")) crawler.setCrawlerSetting(OmicHarmonizer.OMIC.MUT);
        if (options.has("LIFT"))runLiftOver();
    }

    public void runLiftOver() throws IOException {
        System.out.println("Run directory: " + finderRootDir);

        List<File> omicFiles = crawler.run(new File(finderRootDir));

        omicFiles.forEach(f -> {
            OmicHarmonizer.OMIC dataType = determineOmicType(f);
            ArrayList<ArrayList<String>> sheet = getSheet(f);
            logPreLiftStatistics(f,sheet);
            ArrayList<ArrayList<String>> outFile;

            try {
                outFile = harmonizer.runLiftOver(sheet, f.toString(), dataType);
                if (!(outFile.size() == 1 || outFile.isEmpty()))
                    makeOutFileDirAndSave(f, outFile);
                else
                    log.info(String.format("No data lifted for %s",f.getCanonicalPath()));
            } catch (IOException e) {
                log.error(e.toString());
            }
        });
    }

    private void logPreLiftStatistics(File file, ArrayList<ArrayList<String>> sheet) {
        String stats = String.format("%s Contains %d preliftedData points", file.getName(), sheet.size());
    }

    private void makeOutFileDirAndSave(File f,ArrayList<ArrayList<String>> liftedSheet) throws IOException {

        Path filePath = Paths.get(f.toURI());

        String sourceDir = filePath.getParent().getParent().getFileName().toString();
        String parentDataDir = filePath.getParent().getFileName().toString();

        Path outputRoot = Paths.get(URI.create("file://" + finderRootDir));
        Path updog = Paths.get(outputRoot.toString() + "/data/UPDOG");
        Path sourceFolder;

        if(!outputRoot.toFile().exists()) Files.createDirectory(outputRoot);
        if(updog.toFile().exists()) sourceFolder = Paths.get(updog.toString() + "/" + sourceDir);
        else sourceFolder = outputRoot;

        Path sourceData = Paths.get(sourceFolder.toString() + "/" + parentDataDir);
        Path outFile = Paths.get(sourceData.toString() + "/"+f.getName() + ".lfted");

        if(!sourceFolder.toFile().exists())
            Files.createDirectory(sourceFolder);
        if(!sourceData.toFile().exists())
            Files.createDirectory(sourceData);

        tsvUtil.writeTSV(liftedSheet, outFile.toString());
    }

    private OmicHarmonizer.OMIC determineOmicType(File f){
       if(f.getParent().matches(".+/mut")) return OmicHarmonizer.OMIC.MUT;
       else if(f.getParent().matches(".+/cna")) return OmicHarmonizer.OMIC.CNA;
       else return null;
    }

    private ArrayList<ArrayList<String>> getSheet(File fileToRead){
        String filename = fileToRead.getName();
        ArrayList<ArrayList<String>> sheet;

        if (filename.matches(".+xlsx")){
            sheet = getXlsxSheet(fileToRead);
        }else if (filename.matches(".+tsv")) {
            sheet = tsvUtil.readCsv(fileToRead.getAbsolutePath(), "\t");
        } else if (filename.matches(".+csv")) {
            sheet = tsvUtil.readCsv(fileToRead.getAbsolutePath(), ",");
        } else {
            System.err.println("FILETYPE NOT SUPPORTED");
            sheet = null;
        }
        return sheet;
    }


    private ArrayList<ArrayList<String>> getXlsxSheet(File f){
        ArrayList<ArrayList<String>> sheet = null;
        try {
            sheet = reader.readFirstSheet(f);
            log.info(String.format("Lifting file %s", f.getCanonicalFile()));
        } catch (IOException e) {
            log.error(e.toString());
        }
        return sheet;
    }

}
