package contract

import org.web3j.crypto.Credentials
import org.web3j.crypto.WalletUtils
import org.web3j.crypto.TransactionEncoder
import org.web3j.crypto.RawTransaction
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.methods.response.TransactionReceipt
import org.web3j.protocol.http.HttpService
import org.web3j.abi.FunctionEncoder
import org.web3j.tx.Transfer
import org.web3j.tx.gas.ContractGasProvider
import org.web3j.tx.gas.DefaultGasProvider
import org.web3j.tx.gas.StaticGasProvider
import org.web3j.tx.TransactionManager
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.core.methods.response.EthGetTransactionCount
import org.web3j.protocol.core.methods.response.EthGetTransactionReceipt
import org.web3j.protocol.core.methods.response.EthSendTransaction
import org.web3j.protocol.core.methods.response.TransactionReceipt
import org.web3j.protocol.Web3j
import org.web3j.utils.Convert
import org.web3j.utils.Numeric
import org.web3j.utils.Convert.Unit
import org.web3j.protocol.core.methods.request.EthFilter
import org.web3j.protocol.core.methods.response.Log
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.core.DefaultBlockParameterNumber
import org.web3j.crypto.Credentials
import org.web3j.crypto.WalletUtils
import org.web3j.crypto.TransactionEncoder
import org.web3j.crypto.RawTransaction
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.methods.response.TransactionReceipt
import org.web3j.protocol.http.HttpService
import org.web3j.abi.FunctionEncoder
import org.web3j.tx.Transfer
import org.web3j.tx.gas.ContractGasProvider
import org.web3j.tx.gas.DefaultGasProvider
import org.web3j.tx.gas.StaticGasProvider
import org.web3j.tx.TransactionManager
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.core.methods.response.EthGetTransactionCount
import org.web3j.protocol.core.methods.response.EthGetTransactionReceipt
import org.web3j.protocol.core.methods.response.EthSendTransaction
import org.web3j.protocol.core.methods.response.TransactionReceipt
import org.web3j.protocol.Web3j
import org.web3j.utils.Convert
import org.web3j.utils.Numeric
import org.web3j.utils.Convert.Unit
import org.web3j.protocol.core.methods.request.EthFilter
import org.web3j.protocol.core.methods.response.Log
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.core.DefaultBlockParameterNumber
import play.utils.Threads
import scala.util.Random
import java.math.BigInteger
import java.util.concurrent.CompletableFuture;


class StatusEnum extends Enumeration{
  type StatusEnum = Value
  val trusted = Value(1)
  val rejected = Value(0)
  val pending = Value(2)
}


class localGanaceDeploy(accountNumber: String, contractAddress: String,
                        privateKey: String) {

  val statusEnum = new StatusEnum
  var status = statusEnum.pending
  var contract: CRR = _;
  var client: Web3j = _;
  var nonceSync: BigInteger = _;
  var credentials: Credentials = _;
  var gasProvider: ContractGasProvider = _;
  var rnd = new Random();

  def loadDeployedContract(): Any = {
    val w3 = Web3j.build(new HttpService("http://127.0.0.1:7545"));
    this.client = w3;
    println("[deploy] Connected to http provider service");
    this.gasProvider = new StaticGasProvider(new BigInteger("20000000000"),new BigInteger("6721975"));
    this.credentials = Credentials.create(this.privateKey);
    val address = credentials.getAddress();
    println(s"[deploy] computed address: ${address} for private key ${this.privateKey}")
    if(this.accountNumber == address){
      println(s"[deploy] extected address occured from provided private key :) ")
    }
    val contract = CRR.load(this.contractAddress , w3, credentials, gasProvider);
    this.contract = contract;
    this.nonceSync = getNonce()
    val contractAddressCheck = this.contract.getContractAddress();
    println(s"[deploy] contract address matching: ${contractAddressCheck == this.contractAddress}")
  }

  def getNonce() : BigInteger = {
    val getNonceInst = this.client.ethGetTransactionCount(
      this.credentials.getAddress(), DefaultBlockParameterName.LATEST).send();
    getNonceInst.getTransactionCount();
  }

  def initializeContext(taskId: Int,  minDeposit: Int, weiValue: Int): Any = {
    val taskIdBig = new BigInteger(taskId.toString);
    val minDepositBig = new BigInteger(minDeposit.toString);
    val weiValueBig = new BigInteger(weiValue.toString);
    val transaction = this.contract.initializeContext(taskIdBig, minDepositBig, weiValueBig);
    println(s"[initialize contract] transactio obj: $transaction")
    transaction.send();
  }

  def getBalance(): BigInteger = {
    val res = this.contract.getContractBalance().send();
    return res
  }

  def registerExecutor(taskId: Int, weiValue: Int): Any = {
    val taskIdBig = new BigInteger(taskId.toString);
    val weiValueBig = new BigInteger(weiValue.toString);
    val transaction = this.contract.registerExecutor(taskIdBig, weiValueBig);
    transaction.send();
  }

  def submitResults(taskId: Int, resultHash: String, weiValue: Int):  EthSendTransaction = {
    //org.web3j.protocol.core.methods.response.EthSendTransaction
    val taskIdBig = new BigInteger(taskId.toString);
    val resHashBig = new BigInteger(resultHash.toString);
    val weiValueBig = new BigInteger(weiValue.toString);
    val data = this.contract.submitResults(taskIdBig, resHashBig ,weiValueBig);
    this.synchronized {
      val transaction = RawTransaction.createTransaction(
        getNonce(),
        this.gasProvider.getGasPrice(),
        new BigInteger("2100000"),
        this.contract.getContractAddress(),
        weiValueBig,
        data);
      val signed = TransactionEncoder.signMessage(transaction, this.credentials)
      val hexValue : String = Numeric.toHexString(signed);
      this.client.ethSendRawTransaction(hexValue).send();
    }

    //this.contract.submitResults(taskIdBig, resHashBig, weiValueBig).send();
  }

  def pollForNewTaskEvents(): Any ={
    val filter: EthFilter = new EthFilter(DefaultBlockParameterName.EARLIEST,
      DefaultBlockParameterName.LATEST, this.contractAddress.substring(2));
    val a = this.contract.newTaskEventFlowable(filter)
      .subscribe(ev => println(s"Got a newTask event for task-${ev.tid}") );
  }

  def pollForBadResultEvent(): Any = {
    val filter: EthFilter = new EthFilter(DefaultBlockParameterName.PENDING,
      DefaultBlockParameterName.LATEST, this.contractAddress.substring(2));
    val a = this.contract.badResultEventFlowable(filter)
      .subscribe(ev => {
        println(s"==========================================")
        println(s"|GOT A BAD RESULT EVENT FOR TASK-${ev.tid}")
        println(s"==========================================")
        this.status = statusEnum.rejected;
      });
  }

  def startJob(): Any = {
    this.contract.startJob().send();
  }
}

/*
object localGanaceDeploy{
  def apply(accountNumber: String, contractAddress: String, privateKey: String): localGanaceDeploy = {
    new localGanaceDeploy(accountNumber, contractAddress, privateKey)
  }
}*/