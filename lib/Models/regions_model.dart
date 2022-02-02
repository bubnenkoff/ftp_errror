class RegionsModel {
  int id;
  String ru_name;
  String? translite_name;

  RegionsModel(this.id, this.ru_name, this.translite_name);

  Map<String, dynamic> toJson() => {
        'id': id,
        'ru_name': ru_name,
        'translite_name': translite_name,
      };

  // на входе от БД прилетает масив массивов: 
  // [[ ], [ ], [ ]]
  static List<Map<dynamic, dynamic>> listFromArrays(lists) {
    var result = <Map<dynamic, dynamic>>[];
    for (List list in lists) {
       Map<dynamic, dynamic> el = RegionsModel(list[0], list[1], list[2]).toJson();
       result.add(el);
    }
    return result;
  }
}
