package contract;

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
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.DefaultBlockParameterNumber;
import java.math.BigInteger;
import java.util.concurrent.CountDownLatch;



class localGanaceDeploy(accountNumber: String, contractAddress: String,
                              privateKey: String) {

  var contract: CRR = _;
  var client: Web3j = _;

  def loadDeployedContract(): Any = {
    val w3 = Web3j.build(new HttpService("http://127.0.0.1:7545"));
    this.client = w3;
    println("[deploy] Connected to http provider service");
    val gasProvider = new StaticGasProvider(new BigInteger("20000000000"),new BigInteger("6721975"));
    val credentials = Credentials.create(this.privateKey)
    val address = credentials.getAddress()
    println(s"[deploy] computed address: ${address} for private key ${this.privateKey}")
    if(this.accountNumber == address){
      println(s"[deploy] extected address occured from provided private key :) ")
    }
    val contract = CRR.load(this.contractAddress , w3, credentials, gasProvider);
    this.contract = contract;
    val contractAddress: String = contract.getContractAddress();
    println(s"[deploy] contract address matching: ${contractAddress == this.contractAddress}")
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

  def pollForNewTaskEvents(): Any ={
    val filter: EthFilter = new EthFilter(DefaultBlockParameterName.EARLIEST,
        DefaultBlockParameterName.LATEST, this.contractAddress.substring(2));
    val a = this.contract.newTaskEventFlowable(filter)
      .subscribe(ev => println(s"Got a newTask event for task-${ev.tid}"));
  }



}