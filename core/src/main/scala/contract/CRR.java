import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.web3j.abi.EventEncoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
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
    public static final String BINARY = "608060405234801561001057600080fd5b50600080546001600160a01b03191633178155600181905560058190556002556008805460ff191690556105a0806100496000396000f3fe6080604052600436106100705760003560e01c8063893d20e81161004e578063893d20e8146100d6578063a4d8cf9f14610107578063ccce7e861461010f578063f637ea961461012c57610070565b806319a85cee146100755780634aa2d3311461009a5780636f9fb98a146100c1575b600080fd5b6100986004803603604081101561008b57600080fd5b508035906020013561014f565b005b3480156100a657600080fd5b506100af61029d565b60408051918252519081900360200190f35b3480156100cd57600080fd5b506100af6102a3565b3480156100e257600080fd5b506100eb6102a7565b604080516001600160a01b039092168252519081900360200190f35b6100986102b6565b6100986004803603602081101561012557600080fd5b503561035d565b6100986004803603604081101561014257600080fd5b50803590602001356104f6565b600082815260096020526040902054600211156101b3576040805162461bcd60e51b815260206004820152601b60248201527f77616974696e6720666f72206f74686572206578656375746f72730000000000604482015290519081900360640190fd5b6000828152600a60205260409020546101ec576000828152600a6020908152604082208054600181018255908352912001819055610299565b6000828152600a602052604090205460011415610299576000828152600a60205260408120805483929061021c57fe5b90600052602060002001541415610265576040805183815290517ff9e4dee4a5cc23666c62364fb0568dcd7894c6a5ddd71c7d1be396f13edb161d9181900360200190a1610299565b6040805183815290517f5c294404c99ce75d2421c81d85219cee4cf20cae2e12e44b2d966a69d4a9a5e69181900360200190a15b5050565b60055490565b4790565b6000546001600160a01b031690565b600060065411610303576040805162461bcd60e51b81526020600482015260136024820152721a9bd8881b9bdd081e595d081cdd185c9d1959606a1b604482015290519081900360640190fd5b6007546006541461034e576040805162461bcd60e51b815260206004820152601060248201526f6a6f62206e6f742079657420646f6e6560801b604482015290519081900360640190fd5b6008805460ff19169055600080fd5b60045434101561036c57600080fd5b6000818152600960205260409020546002116103c3576040805162461bcd60e51b81526020600482015260116024820152701c9959da5cdd1c985d1a5bdb88199d5b1b607a1b604482015290519081900360640190fd5b60008181526009602052604090205461043f57600081815260096020908152604080832080546001810182559084529282902090920180546001600160a01b03191633179055815183815291517fd4d94e00d7f2d81e1528ab2525d796ee69bebcdefa742063510a70e47fa3918f9281900390910190a16104f3565b600081815260096020526040902054600114156104bf57600081815260096020908152604080832080546001810182559084529282902090920180546001600160a01b03191633179055815183815291517f6152d5b257cc8f258361a4952d20583c8f6e333297294aacd9423afa38e8eb319281900390910190a16104f3565b6040805182815290517f5cea392268772376fff0b079090e4acc87ac05bf925349695372a3040fe075369181900360200190a15b50565b6000546001600160a01b0316331461050d57600080fd5b346003819055600482905533311161052457600080fd5b600354604080518481526020810192909252818101839052517f4470fbfd43622f9df45272968e4611be245225bc7816e1484843c7e7852d38349181900360600190a1505056fea265627a7a72315820ef711892cd42c8db3ab84a70069d62378548676d2aa0a401104c004f0d12256b64736f6c63430005100032";

    public static final String FUNC_GETCONTRACTBALANCE = "getContractBalance";

    public static final String FUNC_GETOWNER = "getOwner";

    public static final String FUNC_GETTOTALPARTICIPANTSCOUNT = "getTotalParticipantsCount";

    public static final String FUNC_INITIALIZECONTEXT = "initializeContext";

    public static final String FUNC_PAYEXECUTORS = "payExecutors";

    public static final String FUNC_REGISTEREXECUTOR = "registerExecutor";

    public static final String FUNC_SUBMITRESULTS = "submitResults";

    public static final Event BADRESULT_EVENT = new Event("badResult", 
            Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
    ;

    public static final Event NEWTASK_EVENT = new Event("newTask", 
            Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}, new TypeReference<Uint256>() {}, new TypeReference<Uint256>() {}));
    ;

    public static final Event REGISTERCOMPLETED_EVENT = new Event("registerCompleted", 
            Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
    ;

    public static final Event REGISTERFULL_EVENT = new Event("registerFull", 
            Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
    ;

    public static final Event REGISTEREDFOR_EVENT = new Event("registeredFor", 
            Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
    ;

    public static final Event T_EVENT = new Event("t", 
            Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
    ;

    public static final Event TASKRESULT_EVENT = new Event("taskResult", 
            Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}, new TypeReference<Uint256>() {}));
    ;

    public static final Event VERIFIEDRESULT_EVENT = new Event("verifiedResult", 
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

    public List<BadResultEventResponse> getBadResultEvents(TransactionReceipt transactionReceipt) {
        List<Contract.EventValuesWithLog> valueList = extractEventParametersWithLog(BADRESULT_EVENT, transactionReceipt);
        ArrayList<BadResultEventResponse> responses = new ArrayList<BadResultEventResponse>(valueList.size());
        for (Contract.EventValuesWithLog eventValues : valueList) {
            BadResultEventResponse typedResponse = new BadResultEventResponse();
            typedResponse.log = eventValues.getLog();
            typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
            responses.add(typedResponse);
        }
        return responses;
    }

    public Flowable<BadResultEventResponse> badResultEventFlowable(EthFilter filter) {
        return web3j.ethLogFlowable(filter).map(new Function<Log, BadResultEventResponse>() {
            @Override
            public BadResultEventResponse apply(Log log) {
                Contract.EventValuesWithLog eventValues = extractEventParametersWithLog(BADRESULT_EVENT, log);
                BadResultEventResponse typedResponse = new BadResultEventResponse();
                typedResponse.log = log;
                typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                return typedResponse;
            }
        });
    }

    public Flowable<BadResultEventResponse> badResultEventFlowable(DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        EthFilter filter = new EthFilter(startBlock, endBlock, getContractAddress());
        filter.addSingleTopic(EventEncoder.encode(BADRESULT_EVENT));
        return badResultEventFlowable(filter);
    }

    public List<NewTaskEventResponse> getNewTaskEvents(TransactionReceipt transactionReceipt) {
        List<Contract.EventValuesWithLog> valueList = extractEventParametersWithLog(NEWTASK_EVENT, transactionReceipt);
        ArrayList<NewTaskEventResponse> responses = new ArrayList<NewTaskEventResponse>(valueList.size());
        for (Contract.EventValuesWithLog eventValues : valueList) {
            NewTaskEventResponse typedResponse = new NewTaskEventResponse();
            typedResponse.log = eventValues.getLog();
            typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
            typedResponse._reward = (BigInteger) eventValues.getNonIndexedValues().get(1).getValue();
            typedResponse._min_deposit = (BigInteger) eventValues.getNonIndexedValues().get(2).getValue();
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
                typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                typedResponse._reward = (BigInteger) eventValues.getNonIndexedValues().get(1).getValue();
                typedResponse._min_deposit = (BigInteger) eventValues.getNonIndexedValues().get(2).getValue();
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
            typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
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
                typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                return typedResponse;
            }
        });
    }

    public Flowable<RegisterCompletedEventResponse> registerCompletedEventFlowable(DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        EthFilter filter = new EthFilter(startBlock, endBlock, getContractAddress());
        filter.addSingleTopic(EventEncoder.encode(REGISTERCOMPLETED_EVENT));
        return registerCompletedEventFlowable(filter);
    }

    public List<RegisterFullEventResponse> getRegisterFullEvents(TransactionReceipt transactionReceipt) {
        List<Contract.EventValuesWithLog> valueList = extractEventParametersWithLog(REGISTERFULL_EVENT, transactionReceipt);
        ArrayList<RegisterFullEventResponse> responses = new ArrayList<RegisterFullEventResponse>(valueList.size());
        for (Contract.EventValuesWithLog eventValues : valueList) {
            RegisterFullEventResponse typedResponse = new RegisterFullEventResponse();
            typedResponse.log = eventValues.getLog();
            typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
            responses.add(typedResponse);
        }
        return responses;
    }

    public Flowable<RegisterFullEventResponse> registerFullEventFlowable(EthFilter filter) {
        return web3j.ethLogFlowable(filter).map(new Function<Log, RegisterFullEventResponse>() {
            @Override
            public RegisterFullEventResponse apply(Log log) {
                Contract.EventValuesWithLog eventValues = extractEventParametersWithLog(REGISTERFULL_EVENT, log);
                RegisterFullEventResponse typedResponse = new RegisterFullEventResponse();
                typedResponse.log = log;
                typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                return typedResponse;
            }
        });
    }

    public Flowable<RegisterFullEventResponse> registerFullEventFlowable(DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        EthFilter filter = new EthFilter(startBlock, endBlock, getContractAddress());
        filter.addSingleTopic(EventEncoder.encode(REGISTERFULL_EVENT));
        return registerFullEventFlowable(filter);
    }

    public List<RegisteredForEventResponse> getRegisteredForEvents(TransactionReceipt transactionReceipt) {
        List<Contract.EventValuesWithLog> valueList = extractEventParametersWithLog(REGISTEREDFOR_EVENT, transactionReceipt);
        ArrayList<RegisteredForEventResponse> responses = new ArrayList<RegisteredForEventResponse>(valueList.size());
        for (Contract.EventValuesWithLog eventValues : valueList) {
            RegisteredForEventResponse typedResponse = new RegisteredForEventResponse();
            typedResponse.log = eventValues.getLog();
            typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
            responses.add(typedResponse);
        }
        return responses;
    }

    public Flowable<RegisteredForEventResponse> registeredForEventFlowable(EthFilter filter) {
        return web3j.ethLogFlowable(filter).map(new Function<Log, RegisteredForEventResponse>() {
            @Override
            public RegisteredForEventResponse apply(Log log) {
                Contract.EventValuesWithLog eventValues = extractEventParametersWithLog(REGISTEREDFOR_EVENT, log);
                RegisteredForEventResponse typedResponse = new RegisteredForEventResponse();
                typedResponse.log = log;
                typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                return typedResponse;
            }
        });
    }

    public Flowable<RegisteredForEventResponse> registeredForEventFlowable(DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        EthFilter filter = new EthFilter(startBlock, endBlock, getContractAddress());
        filter.addSingleTopic(EventEncoder.encode(REGISTEREDFOR_EVENT));
        return registeredForEventFlowable(filter);
    }

    public List<TEventResponse> getTEvents(TransactionReceipt transactionReceipt) {
        List<Contract.EventValuesWithLog> valueList = extractEventParametersWithLog(T_EVENT, transactionReceipt);
        ArrayList<TEventResponse> responses = new ArrayList<TEventResponse>(valueList.size());
        for (Contract.EventValuesWithLog eventValues : valueList) {
            TEventResponse typedResponse = new TEventResponse();
            typedResponse.log = eventValues.getLog();
            typedResponse.p = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
            responses.add(typedResponse);
        }
        return responses;
    }

    public Flowable<TEventResponse> tEventFlowable(EthFilter filter) {
        return web3j.ethLogFlowable(filter).map(new Function<Log, TEventResponse>() {
            @Override
            public TEventResponse apply(Log log) {
                Contract.EventValuesWithLog eventValues = extractEventParametersWithLog(T_EVENT, log);
                TEventResponse typedResponse = new TEventResponse();
                typedResponse.log = log;
                typedResponse.p = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                return typedResponse;
            }
        });
    }

    public Flowable<TEventResponse> tEventFlowable(DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        EthFilter filter = new EthFilter(startBlock, endBlock, getContractAddress());
        filter.addSingleTopic(EventEncoder.encode(T_EVENT));
        return tEventFlowable(filter);
    }

    public List<TaskResultEventResponse> getTaskResultEvents(TransactionReceipt transactionReceipt) {
        List<Contract.EventValuesWithLog> valueList = extractEventParametersWithLog(TASKRESULT_EVENT, transactionReceipt);
        ArrayList<TaskResultEventResponse> responses = new ArrayList<TaskResultEventResponse>(valueList.size());
        for (Contract.EventValuesWithLog eventValues : valueList) {
            TaskResultEventResponse typedResponse = new TaskResultEventResponse();
            typedResponse.log = eventValues.getLog();
            typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
            typedResponse.results = (BigInteger) eventValues.getNonIndexedValues().get(1).getValue();
            responses.add(typedResponse);
        }
        return responses;
    }

    public Flowable<TaskResultEventResponse> taskResultEventFlowable(EthFilter filter) {
        return web3j.ethLogFlowable(filter).map(new Function<Log, TaskResultEventResponse>() {
            @Override
            public TaskResultEventResponse apply(Log log) {
                Contract.EventValuesWithLog eventValues = extractEventParametersWithLog(TASKRESULT_EVENT, log);
                TaskResultEventResponse typedResponse = new TaskResultEventResponse();
                typedResponse.log = log;
                typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                typedResponse.results = (BigInteger) eventValues.getNonIndexedValues().get(1).getValue();
                return typedResponse;
            }
        });
    }

    public Flowable<TaskResultEventResponse> taskResultEventFlowable(DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        EthFilter filter = new EthFilter(startBlock, endBlock, getContractAddress());
        filter.addSingleTopic(EventEncoder.encode(TASKRESULT_EVENT));
        return taskResultEventFlowable(filter);
    }

    public List<VerifiedResultEventResponse> getVerifiedResultEvents(TransactionReceipt transactionReceipt) {
        List<Contract.EventValuesWithLog> valueList = extractEventParametersWithLog(VERIFIEDRESULT_EVENT, transactionReceipt);
        ArrayList<VerifiedResultEventResponse> responses = new ArrayList<VerifiedResultEventResponse>(valueList.size());
        for (Contract.EventValuesWithLog eventValues : valueList) {
            VerifiedResultEventResponse typedResponse = new VerifiedResultEventResponse();
            typedResponse.log = eventValues.getLog();
            typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
            responses.add(typedResponse);
        }
        return responses;
    }

    public Flowable<VerifiedResultEventResponse> verifiedResultEventFlowable(EthFilter filter) {
        return web3j.ethLogFlowable(filter).map(new Function<Log, VerifiedResultEventResponse>() {
            @Override
            public VerifiedResultEventResponse apply(Log log) {
                Contract.EventValuesWithLog eventValues = extractEventParametersWithLog(VERIFIEDRESULT_EVENT, log);
                VerifiedResultEventResponse typedResponse = new VerifiedResultEventResponse();
                typedResponse.log = log;
                typedResponse.tid = (BigInteger) eventValues.getNonIndexedValues().get(0).getValue();
                return typedResponse;
            }
        });
    }

    public Flowable<VerifiedResultEventResponse> verifiedResultEventFlowable(DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        EthFilter filter = new EthFilter(startBlock, endBlock, getContractAddress());
        filter.addSingleTopic(EventEncoder.encode(VERIFIEDRESULT_EVENT));
        return verifiedResultEventFlowable(filter);
    }

    public RemoteFunctionCall<BigInteger> getContractBalance() {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(FUNC_GETCONTRACTBALANCE, 
                Arrays.<Type>asList(), 
                Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
        return executeRemoteCallSingleValueReturn(function, BigInteger.class);
    }

    public RemoteFunctionCall<String> getOwner() {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(FUNC_GETOWNER, 
                Arrays.<Type>asList(), 
                Arrays.<TypeReference<?>>asList(new TypeReference<Address>() {}));
        return executeRemoteCallSingleValueReturn(function, String.class);
    }

    public RemoteFunctionCall<BigInteger> getTotalParticipantsCount() {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(FUNC_GETTOTALPARTICIPANTSCOUNT, 
                Arrays.<Type>asList(), 
                Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
        return executeRemoteCallSingleValueReturn(function, BigInteger.class);
    }

    public RemoteFunctionCall<TransactionReceipt> initializeContext(BigInteger _taskId, BigInteger _min_deposit, BigInteger weiValue) {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(
                FUNC_INITIALIZECONTEXT, 
                Arrays.<Type>asList(new org.web3j.abi.datatypes.generated.Uint256(_taskId), 
                new org.web3j.abi.datatypes.generated.Uint256(_min_deposit)), 
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function, weiValue);
    }

    public RemoteFunctionCall<TransactionReceipt> payExecutors(BigInteger weiValue) {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(
                FUNC_PAYEXECUTORS, 
                Arrays.<Type>asList(), 
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function, weiValue);
    }

    public RemoteFunctionCall<TransactionReceipt> registerExecutor(BigInteger taskId, BigInteger weiValue) {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(
                FUNC_REGISTEREXECUTOR, 
                Arrays.<Type>asList(new org.web3j.abi.datatypes.generated.Uint256(taskId)), 
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function, weiValue);
    }

    public RemoteFunctionCall<TransactionReceipt> submitResults(BigInteger taskId, BigInteger resultHash, BigInteger weiValue) {
        final org.web3j.abi.datatypes.Function function = new org.web3j.abi.datatypes.Function(
                FUNC_SUBMITRESULTS, 
                Arrays.<Type>asList(new org.web3j.abi.datatypes.generated.Uint256(taskId), 
                new org.web3j.abi.datatypes.generated.Uint256(resultHash)), 
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function, weiValue);
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

    public static class BadResultEventResponse extends BaseEventResponse {
        public BigInteger tid;
    }

    public static class NewTaskEventResponse extends BaseEventResponse {
        public BigInteger tid;

        public BigInteger _reward;

        public BigInteger _min_deposit;
    }

    public static class RegisterCompletedEventResponse extends BaseEventResponse {
        public BigInteger tid;
    }

    public static class RegisterFullEventResponse extends BaseEventResponse {
        public BigInteger tid;
    }

    public static class RegisteredForEventResponse extends BaseEventResponse {
        public BigInteger tid;
    }

    public static class TEventResponse extends BaseEventResponse {
        public BigInteger p;
    }

    public static class TaskResultEventResponse extends BaseEventResponse {
        public BigInteger tid;

        public BigInteger results;
    }

    public static class VerifiedResultEventResponse extends BaseEventResponse {
        public BigInteger tid;
    }
}
