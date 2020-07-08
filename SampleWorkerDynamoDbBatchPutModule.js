module.exports = function SampleWorkerDynamoDbBatchPutModule() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = require("./AbstractWorkerDynamoDbBatchPutCommon.js");
  // prototypeにセットする事で継承関係のように挙動させる
  SampleWorkerDynamoDbBatchPutModule.prototype = new superClazzFunc();

  function* execute(event, context, RequireObjects) {
    var base = SampleWorkerDynamoDbBatchPutModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("SampleWorkerDynamoDbBatchPutModule# execute : start");

      return yield SampleWorkerDynamoDbBatchPutModule.prototype.executeBizWorkerCommon(
        event,
        context,
        RequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("SampleWorkerDynamoDbBatchPutModule# execute : end");
    }
  }
  SampleWorkerDynamoDbBatchPutModule.prototype.execute = execute;

  /*
  行データ変換処理

  @param record ファイルから読み込んだ1行データ
  */
  SampleWorkerDynamoDbBatchPutModule.prototype.AbstractBaseCommon.convertRecordData = function (
    record
  ) {
    var base = SampleWorkerDynamoDbBatchPutModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "SampleWorkerDynamoDbBatchPutModule# convertRecordData : start"
      );

      var argLine = String(record);
      base.writeLogTrace(
        "SampleWorkerDynamoDbBatchPutModule# argLine" + argLine
      );
      var values = argLine.split(",");

      var primaryKey = base.getPrimaryKey();
      var sortKey = base.getSortKey();

      var rtnObj = {};
      rtnObj[primaryKey] = { S: values[0] };
      rtnObj[sortKey] = { S: values[1] };
      rtnObj.ColumnB = { S: values[2] };

      base.writeLogTrace(
        "SampleWorkerDynamoDbBatchPutModule# JSON :" + JSON.stringify(rtnObj)
      );

      return rtnObj;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "SampleWorkerDynamoDbBatchPutModule# convertRecordData : end"
      );
    }
  }.bind(SampleWorkerDynamoDbBatchPutModule.prototype.AbstractBaseCommon);

  // 関数定義は　return　より上部に記述
  // 外部から実行できる関数をreturnすること
  return {
    execute,
    SampleWorkerDynamoDbBatchPutModule,
    AbstractBaseCommon:
      SampleWorkerDynamoDbBatchPutModule.prototype.AbstractBaseCommon,
    AbstractBizCommon:
      SampleWorkerDynamoDbBatchPutModule.prototype.AbstractBizCommon,
    AbstractWorkerChildCommon: SampleWorkerDynamoDbBatchPutModule.prototype,
  };
};
