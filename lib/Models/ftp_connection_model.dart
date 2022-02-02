class FTPCredentialsModel {
    late String rootFtpDir;
    late String ftpUrl;
    late String ftpLogin;
    late String ftpPass;

    FTPCredentialsModel({
      required this.rootFtpDir, 
      required this.ftpUrl, 
      required this.ftpLogin, 
      required this.ftpPass}
      );

    FTPCredentialsModel.fromMap(Map map){
      rootFtpDir = map['rootFtpDir'];
      ftpUrl = map['ftpUrl'];
      ftpLogin = map['ftpLogin'];
      ftpPass = map['ftpPass'];
    }

}
