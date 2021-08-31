package org.apache.spark.contract;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.web3j.abi.EventEncoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.RemoteCall;
import org.web3j.protocol.core.RemoteFunctionCall;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.BaseEventResponse;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import org.web3j.tx.Contract;
import org.web3j.tx.TransactionManager;
import org.web3j.tx.gas.ContractGasProvider;

/**
 * <p>Auto generated code.
 * <p><strong>Do not modify!</strong>
 * <p>Please use the <a href="https://docs.web3j.io/command_line.html">web3j command line tools</a>,
 * or the org.web3j.codegen.SolidityFunctionWrapperGenerator in the 
 * <a href="https://github.com/web3j/web3j/tree/master/codegen">codegen module</a> to update.
 *
 * <p>Generated with web3j version 1.4.1.
 */
@SuppressWarnings("rawtypes")
public class CRR extends Contract {
    public static final String BINARY = "608060405234801561001057600080fd5b50600080546001600160a01b03191633178155600181905560028190556003556103fe8061003f6000396000f3fe6080604052600436106100545760003560e01c8062f2a3381461005957806312065fe01461009e5780631b9265b8146100b3578063485315dc146100ca578063a18caf811461010c578063f5ac1a3614610129575b600080fd5b34801561006557600080fd5b5061008c6004803603602081101561007c57600080fd5b50356001600160a01b0316610173565b60408051918252519081900360200190f35b3480156100aa57600080fd5b5061008c610185565b3480156100bf57600080fd5b506100c8610189565b005b3480156100d657600080fd5b506100c8600480360360a08110156100ed57600080fd5b5080359060208101359060408101359060608101359060800135610241565b6100c86004803603602081101561012257600080fd5b50356102b7565b34801561013557600080fd5b5061015f6004803603606081101561014c57600080fd5b5080359060208101359060400135610369565b604080519115158252519081900360200190f35b60076020526000908152604090205481565b4790565b6000546001600160a01b031633146101a057600080fd5b60005b60025481101561023e57600681815481106101ba57fe5b6000918252602082200154600680546001600160a01b03909216926108fc9260079290869081106101e757fe5b60009182526020808320909101546001600160a01b03168352820192909252604090810182205490518115909302929091818181858888f19350505050158015610235573d6000803e3d6000fd5b506001016101a3565b50565b6000546001600160a01b0316331461025857600080fd5b600482905560058190556040805186815260208101869052808201859052606081018490526080810183905290517f88db62829e9b6681455a032d7f0443bcaf35df015b8b6ed643ae216f22f8c0249181900360a00190a15050505050565b6005548110156102c657600080fd5b8034146102d257600080fd5b336006600254815481106102e257fe5b600091825260208083209190910180546001600160a01b0319166001600160a01b039490941693909317909255338152600790915260409020819055600280546001018082551061023e576001600381905560408051918252517f6152d5b257cc8f258361a4952d20583c8f6e333297294aacd9423afa38e8eb319181900360200190a150565b6000600660008154811061037957fe5b6000918252602090912001546001600160a01b03163314806103bc575060066001815481106103a457fe5b6000918252602090912001546001600160a01b031633145b6103c257fe5b939250505056fea265627a7a72315820cea1a3741524d0cacc2890d3318ae24f5d94b50b6d575f517d7ac568c6e5c87764736f6c63430005100032";

    public static final String FUNC_INITIALIZE = "Initialize";

    public static final String FUNC_REGISTER = "Register";

    public static final String FUNC_GETBALANCE = "getBalance";

    public static final String FUNC_PARTICIPANTAMOUNTS = "participantAmounts";

    public static final String FUNC_PAY = "pay";

    public static final String FUNC_RECEIVERESULTS = "receiveResults";

    public static final Event NEWTASK_EVENT = new Event("newTask", 
            Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}, new TypeReference<Uint256>() {}, new TypeReference<Uint256>() {}, new TypeReference<Uint256>() {}, new TypeReference<Uint256>() {}));
    ;

    public static final Event REGISTERCOMPLETED_EVENT = new Event("registerCompleted", 
            Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
    ;

    @Deprecated
    protected CRR(String contractAddress, Web3j web3j, Credentials credentials, BigInteger gasPrice, BigInteger gasLimit) {
        super(BINARY, contractAddress, web3j, credentials, gasPrice, gasLimit);
    }

    protected CRR(String contractAddress, Web3j web3j, Credentials credentials, ContractGasProvider contractGasProvider) {
        super(BINARY, contractAddress, web3j, credentials, contractGasProvider);
    }

    @Deprecated
    protected CRR(String contractAddress, Web3j web3j, TransactionManager transactionManager, BigInteger gasPrice, BigInteger gasLimit) {
        super(BINARY, contractAddress, web3j, transactionManager, gasPrice, gasLimit);
    }

    protected CRR(String contractAddress, Web3j web3j, TransactionManager transactionManager, ContractGasProvider contractGasProvider) {
        super(BINARY, contractAddress, web3j, transactionManager, contractGasProvider);
    }

    public List<NewTaskEventResponse> getNewTaskEvents(TransactionReceipt transactionReceipt) {
        List<Contract.EventValuesWithLog> valueList = extractEventParametersWithLog(NEWTASK_EVENT, transactionReceipt);
        ArrayList<NewTaskEventResponse> responses = new ArrayList<NewTaskEventResponse>(valueList.size());
        for (Contract.EventValuesWithLog eventValues : valueList) {
            NewTaskEventResponse typedResponse = new NewTaskEventResponse();
            typedResponse.log = eventValues.getLog();
            typedResponse._task_url = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
            typedResponse._web_hash = (BigInteger) eventValues.getNonIndexedValues().get(1).getValue();
            typedResponse._comp_hash = (BigInteger) eventValues.getNonIndexedValues().get(2).getValue();
            typedResponse._reward = (BigInteger) eventValues.getNonIndexedValues().get(3).getValue();
            typedResponse._min_deposit = (BigInteger) eventValues.getNonIndexedValues().get(4).getValue();
            responses.add(typedResponse);
        }
        return responses;
    }

    public Flowable<NewTaskEventResponse> newTaskEventFlowable(EthFilter filter) {
        return web3j.ethLogFlowable(filter).map(new Function<Log, NewTaskEventResponse>() {
            @Override
            public NewTaskEventResponse apply(Log log) {
                Contract.EventValuesWithLog eventValues = extractEventParametersWithLog(NEWTASK_EVENT, log);
                NewTaskEventResponse typedResponse = new NewTaskEventResponse();
                typedResponse.log = log;
                typedResponse._task_url = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                typedResponse._web_hash = (BigInteger) eventValues.getNonIndexedValues().get(1).getValue();
                typedResponse._comp_hash = (BigInteger) eventValues.getNonIndexedValues().get(2).getValue();
                typedResponse._reward = (BigInteger) eventValues.getNonIndexedValues().get(3).getValue();
                typedResponse._min_deposit = (BigInteger) eventValues.getNonIndexedValues().get(4).getValue();
                return typedResponse;
            }
        });
    }

    public Flowable<NewTaskEventResponse> newTaskEventFlowable(DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        EthFilter filter = new EthFilter(startBlock, endBlock, getContractAddress());
        filter.addSingleTopic(EventEncoder.encode(NEWTASK_EVENT));
        return newTaskEventFlowable(filter);
    }

    public List<RegisterCompletedEventResponse> getRegisterCompletedEvents(TransactionReceipt transactionReceipt) {
        List<Contract.EventValuesWithLog> valueList = extractEventParametersWithLog(REGISTERCOMPLETED_EVENT, transactionReceipt);
        ArrayList<RegisterCompletedEventResponse> responses = new ArrayList<RegisterCompletedEventResponse>(valueList.size());
        for (Contract.EventValuesWithLog eventValues : valueList) {
            RegisterCompletedEventResponse typedResponse = new RegisterCompletedEventResponse();
            typedResponse.log = eventValues.getLog();
            typedResponse._state = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
            responses.add(typedResponse);
        }
        return responses;
    }

    public Flowable<RegisterCompletedEventResponse> registerCompletedEventFlowable(EthFilter filter) {
        return web3j.ethLogFlowable(filter).map(new Function<Log, RegisterCompletedEventResponse>() {
            @Override
            public RegisterCompletedEventResponse apply(Log log) {
                Contract.EventValuesWithLog eventValues = extractEventParametersWithLog(REGISTERCOMPLETED_EVENT, log);
                RegisterCompletedEventResponse typedResponse = new RegisterCompletedEventResponse();
                typedResponse.log = log;
                typedResponse._state = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                return typedResponse;
            }
        });
    }

    public Flowable<RegisterCompletedEventResponse> registerCompletedEventFlowable(DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        EthFilter filter = new EthFilter(startBlock, endBlock, getContractAddress());
        filter.addSingleTopic(EventEncoder.encode(REGISTERCOMPLETED_EVENT));
        return registerCompletedEventFlowable(filter);
    }

    public RemoteFunctionCall<TransactionReceipt> Initialize(BigInteger _task_url, BigInteger _web_hash, BigInteger _comp_hash, BigInteger _reward, BigInteger _min_deposit) {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(
                FUNC_INITIALIZE, 
                Arrays.<Type>asList(new org.web3j.abi.datatypes.generated.Uint256(_task_url), 
                new org.web3j.abi.datatypes.generated.Uint256(_web_hash), 
                new org.web3j.abi.datatypes.generated.Uint256(_comp_hash), 
                new org.web3j.abi.datatypes.generated.Uint256(_reward), 
                new org.web3j.abi.datatypes.generated.Uint256(_min_deposit)), 
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function);
    }

    public RemoteFunctionCall<TransactionReceipt> Register(BigInteger _amount, BigInteger weiValue) {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(
                FUNC_REGISTER, 
                Arrays.<Type>asList(new org.web3j.abi.datatypes.generated.Uint256(_amount)), 
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function, weiValue);
    }

    public RemoteFunctionCall<BigInteger> getBalance() {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(FUNC_GETBALANCE, 
                Arrays.<Type>asList(), 
                Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
        return executeRemoteCallSingleValueReturn(function, BigInteger.class);
    }

    public RemoteFunctionCall<BigInteger> participantAmounts(String param0) {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(FUNC_PARTICIPANTAMOUNTS, 
                Arrays.<Type>asList(new org.web3j.abi.datatypes.Address(160, param0)), 
                Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
        return executeRemoteCallSingleValueReturn(function, BigInteger.class);
    }

    public RemoteFunctionCall<TransactionReceipt> pay() {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(
                FUNC_PAY, 
                Arrays.<Type>asList(), 
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function);
    }

    public RemoteFunctionCall<TransactionReceipt> receiveResults(BigInteger result, BigInteger merkleHashRoot, BigInteger tapeLength) {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(
                FUNC_RECEIVERESULTS, 
                Arrays.<Type>asList(new org.web3j.abi.datatypes.generated.Uint256(result), 
                new org.web3j.abi.datatypes.generated.Uint256(merkleHashRoot), 
                new org.web3j.abi.datatypes.generated.Uint256(tapeLength)), 
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function);
    }

    @Deprecated
    public static CRR load(String contractAddress, Web3j web3j, Credentials credentials, BigInteger gasPrice, BigInteger gasLimit) {
        return new CRR(contractAddress, web3j, credentials, gasPrice, gasLimit);
    }

    @Deprecated
    public static CRR load(String contractAddress, Web3j web3j, TransactionManager transactionManager, BigInteger gasPrice, BigInteger gasLimit) {
        return new CRR(contractAddress, web3j, transactionManager, gasPrice, gasLimit);
    }

    public static CRR load(String contractAddress, Web3j web3j, Credentials credentials, ContractGasProvider contractGasProvider) {
        return new CRR(contractAddress, web3j, credentials, contractGasProvider);
    }

    public static CRR load(String contractAddress, Web3j web3j, TransactionManager transactionManager, ContractGasProvider contractGasProvider) {
        return new CRR(contractAddress, web3j, transactionManager, contractGasProvider);
    }

    public static RemoteCall<CRR> deploy(Web3j web3j, Credentials credentials, ContractGasProvider contractGasProvider) {
        return deployRemoteCall(CRR.class, web3j, credentials, contractGasProvider, BINARY, "");
    }

    public static RemoteCall<CRR> deploy(Web3j web3j, TransactionManager transactionManager, ContractGasProvider contractGasProvider) {
        return deployRemoteCall(CRR.class, web3j, transactionManager, contractGasProvider, BINARY, "");
    }

    @Deprecated
    public static RemoteCall<CRR> deploy(Web3j web3j, Credentials credentials, BigInteger gasPrice, BigInteger gasLimit) {
        return deployRemoteCall(CRR.class, web3j, credentials, gasPrice, gasLimit, BINARY, "");
    }

    @Deprecated
    public static RemoteCall<CRR> deploy(Web3j web3j, TransactionManager transactionManager, BigInteger gasPrice, BigInteger gasLimit) {
        return deployRemoteCall(CRR.class, web3j, transactionManager, gasPrice, gasLimit, BINARY, "");
    }

    public static class NewTaskEventResponse extends BaseEventResponse {
        public BigInteger _task_url;

        public BigInteger _web_hash;

        public BigInteger _comp_hash;

        public BigInteger _reward;

        public BigInteger _min_deposit;
    }

    public static class RegisterCompletedEventResponse extends BaseEventResponse {
        public BigInteger _state;
    }
}
