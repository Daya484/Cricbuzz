function transform(line) {
    var values = line.split(',');
    var obj = new Object();
    obj.teamName = values[0];
    obj.teamId= values[1];
    obj.imageId= values[2];
    obj.teamSName= values[3];
    obj.ountryName= values[4];
    var jsonString = JSON.stringify(obj);
    return jsonString;
   }
