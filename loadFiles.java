import java.io.*;

import javax.xml.transform.TransformerFactoryConfigurationError;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;


/**
 * loadFiles builds the database by using the database data structure.
 * it builds it from the pdf Files stored in Training data.
 */

class loadFiles{
    String tFile = "Training Data";

    public loadFiles(String tFile){
        this.tFile = tFile;
    }

    public pdfDS buildDs(String[] args) {
        File topFile = new File(tFile);
        pdfDS certDatabase = new pdfDS();
        String[] numWanted = {"250","250","250","250"};
        if(args.length >= 4){
            numWanted = args;
        }
        for(int i = 0; i < 4; i++){
            int filesWanted = Integer.parseInt(numWanted[i]);
            buildSection(certDatabase, topFile, filesWanted, i+1);
        }
        //System.out.println("done");
        //tester4(certDatabase);
        return certDatabase;
    }
    /**
     * builds 1 section of the pdfDS based on how many you want
     * @param cBase
     * @param tFile
     * @param numWanted
     * @param directory
     */
    static void buildSection(pdfDS cBase, File tFile, int numWanted, int directory){
        if(numWanted > 250){
            throw new IllegalArgumentException("cant ask for more than 250 files");
        }
        File[] curDirectoryFiles = tFile.listFiles();
        for(File directoryNumber : curDirectoryFiles){
            if(!(directoryNumber.getName().equals(Integer.toString(directory)))){
                continue;
            }
            int currentNumCounted = 0;
            File[] sublevel2 = directoryNumber.listFiles();

            for(int i=0; i<sublevel2.length; i++){
                if(currentNumCounted == numWanted)
                    break;
                File file = getCorrectFile(sublevel2, currentNumCounted+1, directory);
                if(!(file.isDirectory())){
                    continue;
                }

                //list all files and get the json file
                File[] sublevel3 = file.listFiles();
                File metadataJSONFile = getJSON(sublevel3);
                //parseJSON into array of string arrays
                String[][] realMetaData = parseJSON(metadataJSONFile);
                //for each chunk in chunks load the metadata
                File chunkDirectory = getChunkDirectory(file);
                File[] allChunks = chunkDirectory.listFiles();
                File chunkFile = null;
                for(int j=1; j<= allChunks.length; j++){
                    for(File subFile : allChunks){
                        if(subFile.getName().substring(21).equals(Integer.toString(j))){
                           chunkFile = subFile; 
                           break;
                        }
                    }
                    try{
                    loadChunks(chunkFile, realMetaData[j-1], cBase);
                    }
                    catch(FileNotFoundException e){
                        e.printStackTrace();
                    }
                }

                currentNumCounted++;
            }
        }
    }

    static File getCorrectFile(File[] array, int num, int curSection){
        File checkNull = null;
        for(File file : array){
            if(file.getName().equals(getCFileName(num, curSection))){
                //should always return something
                return file;
            }
        }
        //should never return null
        return checkNull;
    }
    static String getCFileName(int num, int curSection){
        String zeroes = String.format("%0"+ 10 + "d", num);
        return Integer.toString(curSection) + zeroes;
    }
    static File getJSON(File[] array){
        File nullChecker = null;
        for(File file : array){
            if(file.getName().equals("meta.json")){
                //should always return something
                return file;
            }
        }
        //should never return null
        return nullChecker;
    }


    /**
     * converts a jsonarray file into an in order list of strings
     * @param jFile - file containing array
     * @return string[][] of all the elements
     */
    static String[][] parseJSON(File jFile){
        //store names of fields in array in order we want them;
        String[] fieldConstants = {"certificate_type", "certificate_course_name", "certificate_user_name",
        "certificate_completion_date", "certificate_expiration_date", "file_name", "file_upload_date", 
        "file_size", "chunk_file_name"};

        
        //create parser to parse file
        JSONParser parser = new JSONParser();
        String[][] toReturn = new String[0][0]; 
    try{
        //parse file into an object and typecast into array
        Object obj = parser.parse(new FileReader(jFile.getPath()));     
        JSONArray array = (JSONArray) obj;    
        //make size of string array array equal to number of objects in json
        toReturn = new String[array.size()][];
         //for each of the metadata objects make a string[] then add to toReturn
        int count = 0;
        for(Object object : array){
            JSONObject term = (JSONObject) object;
            String[] curLine = new String[term.size()-1];
            for(int i = 0; i < term.size()-1; i++){
                curLine[i] = (String) term.get(fieldConstants[i]);
            }
            curLine[5] = (String) term.get(fieldConstants[8]);
        toReturn[count++] = curLine;
        }

    }catch(Exception pe) {
         pe.printStackTrace();
         System.out.println(pe);
    }

    return toReturn;
    }

    /**
     * given a directory containing a directory with chunk it
     * will always return the chunk directory
     */
    static File getChunkDirectory(File numberDirectory){
        if(!numberDirectory.isDirectory()){
            throw new IllegalArgumentException("not a directory");
        }
        //needs to be initialized so compiler doesn't get mad
        File toReturn = numberDirectory;
        for(File file : numberDirectory.listFiles()){
            if(file.getName().contains("chunks")){
                //should always return something
                toReturn = file;
                return toReturn;
            }
        }
        //probably unnecessary
        System.out.printf("\n\nERROR GETCHUNKDIRECTORY LOADFILES\n\n\n\n\n\n\n\n\n\nx");
        return toReturn;


    }

    /**
     * @param chunkFile
     * @param metaData
     * @throws FileNotFoundException
     */
    public static void loadChunks(File chunkFile, String[] metaData, 
        pdfDS certDatabase) throws FileNotFoundException{

        //turn chunkFile into byteArray then load it with metadata
        try(
            InputStream inputStream = new FileInputStream(chunkFile);
        )
        {
            int chunkSize = (int) chunkFile.length();
            //System.out.println(chunkSize);
            //System.out.println(metaData[5]);
            byte[] byteArray = new byte[chunkSize];     
            int bytesRead = inputStream.read(byteArray);
            certDatabase.insertCertificateChunk(metaData, byteArray);             
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    //==============================================================
    /*
     * Tester functions are contained below this line. 
     * Will be used to test the program
     * 
     * 
     */

    /**
     * prints out all 1st nodes for the Node array.
     * @param certifications
     */
    static void tester1(pdfDS certifications){
        for(pdfDS.Node cur : certifications.allCerts){
            if(cur == null){
                continue;
            }
            for(String s : cur.metaData){
                if(s.contains("chunk")){
                    s = s.split(".ch")[0];
                }
                System.out.printf("%s,",s);
            }
            System.out.printf("\n");
        }
        System.out.printf("\n");
    }

    /**
     * prints out all nodes for 1 value in the Node array.
     * @param certifications
     */
    static void tester2(pdfDS certifications){
        pdfDS.Node cur = certifications.allCerts[0];
        while(cur != null){
            for(String s : cur.metaData){
                System.out.printf(" %s |",s);
            }
            System.out.printf("\n");
            cur = cur.getNext();
        }
    }
    static void tester3(pdfDS certifications){
    String[] searchTerms = {"DBMI", "*", "*"};
    //System.out.println(certifications.returnCertificateMetadata(searchTerms));
    }
    static void tester4(pdfDS certifications){
        String[] searchTerms = {"DBMI", "*", "*",
        "05/10/2022", "05/10/2025", "*", "*"};
        byte[] array = null;
        try{
            array = certifications.getCertificatePDF(searchTerms, true);
        }
        catch(Exception e){
            e.printStackTrace();
        }
        try (OutputStream out = new FileOutputStream("out.pdf")) {
           out.write(array);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}