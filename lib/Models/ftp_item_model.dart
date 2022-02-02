// ignore_for_file: file_names

class FTPItemModel {
    
    String regionName;
    String sectionName;
    String ftpPath;
    String archiveName;
    String archDate;
    String fz;

    FTPItemModel({
      required this.regionName,  
      required this.sectionName, 
      required this.ftpPath, 
      required this.archiveName, 
      required this.archDate,
      required this.fz
      });

}

List<FTPItemModel> ftpItemModelList = [];