# Contributing

All contributions to this project are welcome!

The following internal docs are intended to help first time contributors better 
understand the context of the project, formalise our intentions for the initial development
process, and highlight the most important parts of the code architecture to reduce
barriers to contribution.

## Development Roadmap

Our starting point is the datastore backend in the [Djangae project](https://github.com/potatolondon/djangae). Our step by step plan to port this over and shape it into a standalone app is as follows:

### 1. Port over the existing code and make it compatible with the new APIs

* Use the Google Cloud Python API and not the App Engine specific API. Practically this means we need to replace all `google.appengine.ext` imports to `google.cloud` and use the new API interface this exposes.
* Support the Python 3 runtime (a large portion of the existing Djangae codebase is written to be compatible with both Python 2 and 3 - we don't need to support backports).
* This is intended to be a stand alone app, which means any imports from other Djangae specific modules (such as common utils) will need to be replaced with an equivilant. 

### 2. Update implementation logic based on the new Datastore in Firestore Mode

The [Datastore in Firestore mode introduces a number of new behaviours](https://cloud.google.com/datastore/docs/firestore-or-datastore):

* All Cloud Datastore queries become strongly consistent (so workarounds for eventually consistent queries can be removed)
* Transactions can access any number of entity groups (so any workaround to handle the previous limitation of 25 entities per transaction can potentially be removed)
* Writes to an entity group are no longer limited to 1 per second.

### 3.Refactor and general improvements

Finally we will have chance to improve the readability of the codebase. Some existing ideas include:

* Follow the modular strucutre common to other Django database backends - putting our lower level implementation logic into a seperate directory.
* Introduce a base `Command` class, to handle the common behaviour in all the __init__ methods.
* Re-think the complete implmentation of unique markers.

### Key Components

Highlighting some of the important components within our codebase:

* The local emulator is started via our wrapper around the Django management command in `glcoud.commands.management.__init__`.
* The `datastore.Client` is the base interface exposed by the `google.cloud.datastore` API. This is initialised with the current connection in the `Connection` object inside `base.py`. All roads and RPC calls lead back to this object.
* The old `djangae.rpc` wrapper above the third party API has essentially been replaced by `transaction._rpc` - under the hood this keeps a reference to the `datastore.Client`, and offers a thin wrapper around the Google APIs with our own `get/put/key/query` interface via the `transaction.Transaction` object.
