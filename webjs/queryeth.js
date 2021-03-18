// In Node.js
const Web3 = require("web3");
const fs = require("fs");

// this is the ABI for justinRoiland, "0x5af05b9716a58cba143bddaad541112d8953305e"
// but it works for others because they all have tokenURI
let rawdata = fs.readFileSync("abi.json");
let res = JSON.parse(rawdata);
// console.log(res);
let abi = JSON.parse(res.result);
// console.log("abi", abi)

let web3 = new Web3(
  new Web3.providers.WebsocketProvider(
    "ws://127.0.0.1:8546"
  )
);
var version = web3.version.api;

// niftyGateway = "0xE052113bd7D7700d623414a0a4585BCaE754E9d5"
justinRoiland = "0x5af05b9716a58cba143bddaad541112d8953305e";
collision = "0xe498ec1aff3c1460f6a818826443fd2a7817e775";
allurium = "0x59e0aadb79fe7a00fc4f1564485702804676b918";
dota = "0x3f133816f8178b93ac991a2c3eeddb8f947af0cb";
breakdown = "0xb5a19b9aa60e96b85f0786ac88871cda72cb94c5";
stamp = "0x7e789e2dd1340971de0a9bca35b14ac0939aa330";


const getTotalSupply = async (contract) => {
  let supply = await contract.methods.totalSupply().call();
  // console.log("supply", supply)
  return supply;
};

const getIPFSHash = async (contract, tokenID) => {
  let hash = await contract.methods.tokenIPFSHash(tokenID).call();
  // console.log("returned hash in async", hash);
  return hash;
};

const getContractName = async (contract) => {
  let name = await contract.methods.name().call();
  // console.log("returned name in async", name);
  return name;
};

const getTokenURI = async (contract, tokenID) => {
  let uri = await contract.methods.tokenURI(tokenID).call();
  console.log("returned uri in async", uri);
  return uri;
};

const numNiftiesInContract = async (contract) => {
  let num = await contract.methods.numNiftiesCurrentlyInContract().call();
  // console.log("returned uri in async", num);
  return num;
};

// Putting it together

const getAllHashes = async (contract) => {
  console.log("doingstuff");
  let supply = await getTotalSupply(contract);
  let numHashes = 0;
  try {
    numHashes = await numNiftiesInContract(contract);
    console.log("Num hashes in contract", numHashes);
  } catch {
    console.log("No numHashes field");
  }
  let tokens = [];
  let hashes = [];
  for (var i = 0; i < supply; i++) {
    let id = await contract.methods.tokenByIndex(i).call();
    let obj = {
      tokenID: id,
    };

    let getHashPromise = getIPFSHash(contract, id);
    let getTokenURIPromise = getTokenURI(contract, id);
    try {
      let hash = await getHashPromise;
      obj["IPFSHash"] = hash;
    } catch (e) {
      // console.log("failed to get ipfs hash: ", e);
    }

    uri = await getTokenURIPromise;
    obj["tokenURI"] = uri;
    /*
        if(!hashes.includes(hash)){
          console.log("Yes!")
          hashes.push(hash)
          var obj = {
            "tokenID": id,
            "tokenURI": uri,
            "IPFSHash": hash
          }
          */
    console.log(obj);
    tokens.push(obj);
    /*
        } else {
          console.log("Duplicate hash", hash)
        }
        */

    // If we haven't seen this hash before, put it in obj and hashes array

    if (numHashes != 0) {
      if (hashes.length >= numHashes) break;
    }
  }
  // console.log("tokens", tokens)
  return tokens;
};

const writeFileSync = (fileName, data) => {
  fs.writeFile("data.txt", JSON.stringify(data)+"\n", function (err) {
    if (err) throw err;
    console.log("Saved!");
    process.exit(0);
  });
}

const writeFileStream = async (fileName, data) => {
      // write the contents of the buffer, from position 0 to the end, to the file descriptor returned in opening our file
      fs.appendFile(fileName, JSON.stringify(data)+"\n", function(err) {
          if (err) {
            throw 'error writing file: ' + err;
          }
      });
}

const getContractsFromFile = () => {
  let contracts = [];
  let data = fs.readFileSync('better.txt')
    let lines = data.toString().split("\n")
     for(i in lines) {
         let parts = lines[i].split("\t")
        if (parts[0].includes("0x")) {
          contracts.push(parts[0])
        }

     }
    return contracts
}
// let rawdata = fs.readFileSync("abi.json");

const populateData = async (addr) => {
  var contract = new web3.eth.Contract(abi, addr);

  let contractData = {
    contractAddress: addr,
    contractName: "",
    tokens: [],
  };

  try {
    let gcn = getContractName(contract, addr);
    let gah = getAllHashes(contract, addr);

    contractData.contractAddress = addr;
    contractData.contractName = await gcn;
    contractData.tokens = await gah;
    console.log(contractData);
    writeFileStream("data.txt", contractData)
  } catch (e) {
    console.log(e);
  }
};

// populateData()
(async () => {
  let addrs = getContractsFromFile();
  for (i in addrs) {
    let a = addrs[i];
    console.log("calling populate data on: ", a)
    try {
    await populateData(a);
  } catch(e) {
    console.log("FAILED TO FETCH: ", a)
  }
  }
})()
// getTokenURI()
