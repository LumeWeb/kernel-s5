import { concatBytes, ensureBytes } from "@lumeweb/libkernel";
import {
  addHandler,
  defer,
  getKey,
  handlePresentKey as handlePresentKeyModule,
} from "@lumeweb/libkernel/module";
import type { ActiveQuery } from "@lumeweb/libkernel/module";
import {
  createClient as createSwarmClient,
  SwarmClient,
} from "@lumeweb/kernel-swarm-client";
import Protomux from "@lumeweb/kernel-protomux-client";
import {
  CID,
  CID_HASH_TYPES,
  createKeyPair,
  createNode,
  NodeId,
  S5NodeConfig,
} from "@lumeweb/libs5";
import type { S5Node } from "@lumeweb/libs5";
import KeyPairEd25519 from "@lumeweb/libs5/lib/ed25519.js";
import { Level } from "level";
import HyperTransportPeer from "@lumeweb/libs5-transport-hyper";

const PROTOCOL = "lumeweb.service.s5";

const moduleReadyDefer = defer();

let swarm: SwarmClient;
let node: S5Node;

addHandler("presentKey", handlePresentKey);
addHandler("ready", ready);
addHandler("getRegistryEntry", handleGetRegistryEntry);
addHandler("setRegistryEntry", handleSetRegistryEntry);
addHandler("registrySubscription", handleRegistrySubscription, {
  receiveUpdates: true,
});
addHandler("cat", handleCat);
addHandler("stat", handleStat);

async function handlePresentKey(aq: ActiveQuery) {
  handlePresentKeyModule({
    callerInput: {
      key: aq.callerInput.rootKey,
    },
  } as ActiveQuery);

  await setup();

  moduleReadyDefer.resolve();
}

async function setup() {
  swarm = createSwarmClient();

  const peerConnectedDefer = defer();

  const db = new Level<string, Uint8Array>("s5");
  await db.open();
  let config = {
    keyPair: createKeyPair(await getKey()),
    db,
    p2p: {
      peers: {
        initial: [],
      },
    },
  } as S5NodeConfig;

  swarm.join(PROTOCOL);
  await swarm.start();
  await swarm.ready();

  node = createNode(config);

  await node.start();

  swarm.on("connection", async (peer: any) => {
    const muxer = Protomux.from(peer);
    const s5peer = new HyperTransportPeer({
      muxer,
      peer,
      protocol: PROTOCOL,
    });

    s5peer.id = new NodeId(
      concatBytes(
        Uint8Array.from([CID_HASH_TYPES.ED25519]),
        peer.remotePublicKey,
      ),
    );

    await s5peer.init();
    node.services.p2p.onNewPeer(s5peer, true);
    node.services.p2p.once("peerConnected", peerConnectedDefer.resolve);
  });

  return peerConnectedDefer.promise;
}

async function ready(aq: ActiveQuery) {
  await moduleReadyDefer.promise;

  aq.respond();
}

async function handleGetRegistryEntry(aq: ActiveQuery) {
  if (!("pubkey" in aq.callerInput)) {
    aq.reject("pubkey required");
  }

  await moduleReadyDefer.promise;

  let { pubkey } = aq.callerInput;

  pubkey = ensureBytes("registry entry", pubkey, 33);

  const ret = await node.services.registry.get(pubkey);

  if (!ret) {
    aq.reject("could not find registry entry");
    return;
  }

  aq.respond(ret);
}

async function handleSetRegistryEntry(aq: ActiveQuery) {
  for (const field of ["key", "data", "revision"]) {
    if (!(field in aq.callerInput)) {
      aq.reject(`${field} required`);
      return;
    }
  }

  await moduleReadyDefer.promise;

  let { key, data, revision } = aq.callerInput;

  key = ensureBytes("registry entry private key", key, 32);

  const sre = node.services.registry.signRegistryEntry({
    kp: new KeyPairEd25519(key),
    data,
    revision,
  });

  try {
    await node.services.registry.set(sre);
    aq.respond(sre);
  } catch (e) {
    aq.reject(e);
  }
}

async function handleRegistrySubscription(aq: ActiveQuery) {
  if (!("pubkey" in aq.callerInput)) {
    aq.reject("pubkey required");
  }

  await moduleReadyDefer.promise;

  let { pubkey } = aq.callerInput;

  pubkey = ensureBytes("registry entry ", pubkey, 32);

  const wait = defer();

  const done = node.services.registry.listen(pubkey, (sre) => {
    aq.sendUpdate(sre);
  });

  aq.setReceiveUpdate?.(() => {
    done();
    wait.resolve();
  });

  await wait.promise;
  aq.respond();
}

async function handleCat(aq: ActiveQuery) {
  if (!("cid" in aq.callerInput)) {
    aq.reject("cid required");
    return;
  }

  try {
    const ret = await node.downloadBytesByHash(
      CID.decode(aq.callerInput.cid as string).hash,
    );

    aq.respond(ret);
  } catch (e) {
    aq.reject(e);
  }
}
async function handleStat(aq: ActiveQuery) {
  if (!("cid" in aq.callerInput)) {
    aq.reject("cid required");
    return;
  }

  try {
    const ret = await node.getMetadataByCID(CID.decode(aq.callerInput.cid));
    aq.respond(ret.toJson());
  } catch (e) {
    aq.reject(e);
  }
}
