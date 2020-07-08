module.exports = function AbstractWorkerDynamoDbBatchPutCommon() {
  var Promise;

  // 継承：親クラス＝AbstractBizCommon.js
  var superClazzFunc = require("./AbstractBizCommon.js");
  AbstractWorkerDynamoDbBatchPutCommon.prototype = new superClazzFunc();

  // 各ログレベルの宣言
  var LOG_LEVEL_TRACE = 1;
  var LOG_LEVEL_DEBUG = 2;
  var LOG_LEVEL_INFO = 3;
  var LOG_LEVEL_WARN = 4;
  var LOG_LEVEL_ERROR = 5;

  // 現在の出力レベルを設定(ワーカー処理は別のログレベルを設定可能とする)
  var LOG_LEVEL_CURRENT = LOG_LEVEL_INFO;
  if (process && process.env && process.env.LogLevelForWorker) {
    LOG_LEVEL_CURRENT = process.env.LogLevelForWorker;
  }

  // テーブル名を環境変数から取得
  var TABLE_NAME = "";
  if (process && process.env && process.env.TableName) {
    TABLE_NAME = process.env.TableName;
  }

  // DynamoDBのプライマリーKey
  var PRIMARY_KEY = "";
  if (process && process.env && process.env.PrimaryKey) {
    PRIMARY_KEY = process.env.PrimaryKey;
  }

  // DynamoDBのソートKey
  var SORT_KEY = "";
  if (process && process.env && process.env.SortKey) {
    SORT_KEY = process.env.SortKey;
  }

  // DynamoDB リトライ上限
  var DYNAMO_PUT_FOR_RETRY_MAX = 3;
  if (process && process.env && process.env.DynamoPutForRetryMax) {
    DYNAMO_PUT_FOR_RETRY_MAX = process.env.DynamoPutForRetryMax;
  }

  /*
  現在のログレベルを返却する
  ※　処理制御側とログレベルを同一設定で行う場合は、オーバーライドをコメントアウトする
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.getLogLevelCurrent = function () {
    return LOG_LEVEL_CURRENT;
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype);
  /*
  出力レベル毎のログ処理
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.writeLogTrace = function (
    msg
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelTrace() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype);

  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.writeLogDebug = function (
    msg
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelDebug() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype);

  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.writeLogInfo = function (
    msg
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelInfo() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype);

  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.writeLogWarn = function (
    msg
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelWarn() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype);

  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.writeLogError = function (
    msg
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelError() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype);

  /*
  テーブル名を返却する
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.getTableName = function () {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# getTableName : start"
      );

      return TABLE_NAME;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# getTableName : end"
      );
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon);

  /*
  DynamoDBのプライマリーKeyを返却する
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.getPrimaryKey = function () {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# getPrimaryKey : start"
      );

      return PRIMARY_KEY;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# getPrimaryKey : end"
      );
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon);

  /*
  DynamoDBのソートKeyを返却する
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.getSortKey = function () {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# getSortKey : start"
      );

      return SORT_KEY;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# getSortKey : end"
      );
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon);

  /*
  DynamoDBのput処理　リトライ上限
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.getDynamoPutForRetryMax = function () {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# getDynamoPutForRetryMax : start"
      );

      return DYNAMO_PUT_FOR_RETRY_MAX;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# getDynamoPutForRetryMax : end"
      );
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon);

  // 処理の実行
  function* executeBizWorkerCommon(event, context, bizRequireObjects) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# executeBizWorkerCommon : start"
      );
      if (bizRequireObjects.PromiseObject) {
        Promise = bizRequireObjects.PromiseObject;
      }
      AbstractWorkerDynamoDbBatchPutCommon.prototype.RequireObjects = bizRequireObjects;

      return yield AbstractWorkerDynamoDbBatchPutCommon.prototype.executeBizCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# executeBizWorkerCommon : end"
      );
    }
  }
  AbstractWorkerDynamoDbBatchPutCommon.prototype.executeBizWorkerCommon = executeBizWorkerCommon;

  /*
  業務前処理

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.beforeMainExecute = function (
    args
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# beforeMainExecute : start"
      );

      base.writeLogInfo(
        "AbstractWorkerDynamoDbBatchPutCommon# beforeMainExecute:args:" +
          JSON.stringify(args)
      );

      return Promise.resolve(args);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# beforeMainExecute : end"
      );
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon);

  /*
  行データ変換処理

  @param record ファイルから読み込んだ1行データ
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.convertRecordData = function (
    record
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# convertRecordData : start"
      );

      return JSON.parse(record);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# convertRecordData : end"
      );
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon);

  /*
  S3 SDK の batchWriteItemに渡すJSONを整形する

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.transformRecordInfos = function (
    args
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# transformRecordInfos : start"
      );

      return new Promise(function (resolve, reject) {
        var items = base.getLastIndexObject(args);

        var tableName = base.getTableName();
        var primaryKey = base.getPrimaryKey();
        var sortKey = base.getSortKey();

        // DynamoDB batchPutで登録する時は、1回の登録データ群の中に、
        // 重複キーが存在してはいけないので、データを弾く
        var duplicateCheckMap = {};

        var jsonValues = {};
        jsonValues[tableName] = [];

        for (var i = 0; i < items.length; i++) {
          var line = items[i];

          var item = base.convertRecordData(line);

          if (item && item[primaryKey] && item[sortKey]) {
            var itemKey = item[primaryKey].S + "" + item[sortKey].S;

            if (itemKey in duplicateCheckMap) {
              base.writeLogWarn(
                "AbstractWorkerDynamoDbBatchPutCommon# Duplicate Item :" +
                  JSON.stringify(item)
              );
              continue;
            } else {
              duplicateCheckMap[itemKey] = "1";
            }

            var itemJson = {
              PutRequest: {
                Item: item,
              },
            };

            jsonValues[tableName].push(itemJson);
          }
        }
        var dynamoRequest = {
          RequestItems: jsonValues,
        };

        resolve(dynamoRequest);
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# transformRecordInfos : end"
      );
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon);

  /*
  業務前処理

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.businessMainExecute = function (
    args
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# businessMainExecute : start"
      );

      var dynamoRequest = base.getLastIndexObject(args);

      var keys = Object.keys(dynamoRequest.RequestItems);
      if (keys.length > 0) {
        var retryCount = 0;
        var retryMax = base.getDynamoPutForRetryMax();

        return new Promise(function (resolve, reject) {
          var processItemsCallback = function (err, data) {
            if (err) {
              reject(err);
            } else {
              retryCount++;

              var params = {};
              params.RequestItems = data.UnprocessedItems;

              var chkkeys = Object.keys(params.RequestItems);

              if (chkkeys.length > 0 && retryCount < retryMax) {
                setTimeout(function () {
                  base.RequireObjects.Dynamo.batchWriteItem(
                    params,
                    processItemsCallback
                  );
                }, 1000);
              } else if (retryCount >= retryMax) {
                base.writeLogWarn(
                  "AbstractWorkerDynamoDbBatchPutCommon# Max Retry Over"
                );
                resolve(data);
              } else if (chkkeys.length == 0) {
                base.writeLogInfo(
                  "AbstractWorkerDynamoDbBatchPutCommon# All Data Finish"
                );
                var empty = {};
                resolve(empty);
              }
            }
          };

          base.RequireObjects.Dynamo.batchWriteItem(
            dynamoRequest,
            processItemsCallback
          );
        });
      } else {
        var empty = {};
        return Promise.resolve(empty);
      }
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# businessMainExecute : end"
      );
    }
  }.bind(AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon);

  /*
  最終後処理
  ワーカー処理（疑似スレッド処理）の戻り値を加工して返却する。

  @param event Lambda起動引数
  @param context コンテキスト
  @param results ワーカー処理（疑似スレッド処理）の順次処理結果
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.afterAllTasksExecute = function (
    event,
    context,
    results
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# afterAllTasksExecute : start"
      );

      var dynamoResponse = base.getLastIndexObject(results);

      // 返却用
      var params = {};

      if (dynamoResponse && dynamoResponse.UnprocessedItems) {
        var keys = Object.keys(dynamoResponse.UnprocessedItems);
        if (keys.length > 0) {
          params.RequestItems = dynamoResponse.UnprocessedItems;
          base.writeLogWarn(
            "AbstractWorkerDynamoDbBatchPutCommon# Exists UnprocessedItems"
          );
        }
      }

      return params;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# afterAllTasksExecute : end"
      );
    }
  };

  /*
  順次処理する関数を指定する。
  Promiseを返却すると、Promiseの終了を待った上で順次処理をする
  
  前処理として、initEventParameterを追加

  @param event Lambdaの起動引数：event
  @param context Lambdaの起動引数：context
  */
  AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon.getTasks = function (
    event,
    context
  ) {
    var base =
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerDynamoDbBatchPutCommon# getTasks :start"
      );

      return [
        this.beforeMainExecute,
        this.transformRecordInfos,
        this.businessMainExecute,
      ];
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractWorkerDynamoDbBatchPutCommon# getTasks :end");
    }
  };

  // 関数定義は　return　より上部に記述
  // 外部から実行できる関数をreturnすること
  return {
    executeBizWorkerCommon,
    AbstractBaseCommon:
      AbstractWorkerDynamoDbBatchPutCommon.prototype.AbstractBaseCommon,
    AbstractBizCommon: AbstractWorkerDynamoDbBatchPutCommon.prototype,
  };
};
