import { concatBytes } from "@lumeweb/libkernel";
import {
  addHandler,
  defer,
  getKey,
  handlePresentKey as handlePresentKeyModule,
} from "@lumeweb/libkernel/module";
import type { ActiveQuery } from "@lumeweb/libkernel/module";
import { createClient as createSwarmClient } from "@lumeweb/kernel-swarm-client";
import Protomux from "@lumeweb/kernel-protomux-client";
import {
  createKeyPair,
  createNode,
  NodeId,
  S5NodeConfig,
} from "@lumeweb/libs5";
import { mkeyEd25519 } from "@lumeweb/libs5/lib/constants.js";
import { Level } from "level";
import HyperTransportPeer from "@lumeweb/libs5-transport-hyper";

const PROTOCOL = "lumeweb.service.s5";

const moduleReadyDefer = defer();

let swarm;

addHandler("presentKey", handlePresentKey);
addHandler("ready", ready);

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

  const node = createNode(config);

  await node.start();

  swarm.on("connection", async (peer: any) => {
    const muxer = Protomux.from(peer);
    const s5peer = new HyperTransportPeer({
      muxer,
      peer,
      protocol: PROTOCOL,
    });

    s5peer.id = new NodeId(
      concatBytes(Uint8Array.from([mkeyEd25519]), peer.remotePublicKey),
    );

    await s5peer.init();
    node.services.p2p.onNewPeer(s5peer, true);
  });
}
async function ready(aq: ActiveQuery) {
  await moduleReadyDefer.promise;

  aq.respond();
}
