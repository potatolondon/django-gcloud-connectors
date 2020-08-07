# Django GCloud Connectors (gcloudc)

**Note: This project is now living in [GitLab](https://gitlab.com/potato-oss/google-cloud/django-gcloud-connectors)**

The aim of this project is to create Django database connector / backend for Google Cloud.

Currently it contains a connector for the Google Cloud Datastore (Datastore in Firestore mode)
but in the future it may also include a Firestore connector, or even a MemoryStore one.

This is the continuation of the Datastore connector from the [Djangae project](https://github.com/potatolondon/djangae)
but converted to use the [Cloud Datastore API](https://googleapis.github.io/google-cloud-python/latest/datastore/) on Python 3.

If you are interested in submitting a patch, please refer to `CONTRIBUTING.md`

---

## Looking for Commercial Support?

Potato offers Commercial Support for all its Open Source projects and we can tailor a support package to your needs.

If you're interested in commercial support, training, or consultancy then go ahead and contact us at [opensource@potatolondon.com](mailto:opensource@potatolondon.com)

---


## Running the tests

```
$ pip3 install --user tox
$ tox
```

Under the hood tox runs `./manage.py test`. To pass down arguments to this command simply separate them with a double hyphen. e.g.

```
tox -e py37 -- --failfast
```

# Automatic Cloud Datastore Emulator startup

gcloudc provides overrides for the `runserver` and `test` commands which
start and stop a Cloud Datastore Emulator instance. To enable this functionality add `gcloudc.commands` _at the beginning_ of your `INSTALLED_APPS` setting.

# Release process

Release to pypi is managed by GitLab CI. To create a new release create the relevant tag
and push it to the gitlab remote. But first you should do some version fiddling...

```
1. Update the version in setup.py to the new version by removing the 'a' suffix (most likely)
2. Commit this change
3. Run `git tag -a X.Y.Z -m "Some description"
4. Run `git push origin master && git push --tags`
5. Open setup.py again, bump to the *next* release version, use an 'a' suffix
6. Run `git commit -am "Bump to alpha version" && git push origin master`
```

This will trigger a pipeline that will publish the package in test.pypi.org.
If that is successful, you can then manually trigger the job `publish to prod pypi` on the same pipeline to deploy to the official pypi registry.

# Caveats

It is *strongly recommended* that you read the Cloud Datastore API documentation before using this ORM backend. Understanding of the Datastore
vs SQL will help avoid unexpected surprises!

The Google Cloud Datastore is *not* your traditional SQL database, and for that reason the Datastore backend doesn't support
all of the functionality of the Django ORM (although it supports the majority). Also, some things don't always work the way
you'd expect. As the Datastore is a No-SQL database, anything relying on cross-table queries or aggregates is basically unsupported.

Here are some of the limitations and differences:

 - We ship specialised atomic() decorators, including support for "independent" transactions
 - There is no support for savepoints, nested atomic() blocks are effectively a no-op
 - Django's atomic() decorators WILL NOT WORK
 - No support for select_related(), although prefetch_related() works
 - No support for cross-table ordering
 - Only up-to 500 entities can be read or written inside an atomic() block
 - No support for aggregate queries
 - Queries can only contain a single inequality operation (gt, lt, lte, gte, isnull=False), and the resultset must be ordered by the field you're testing for inequality

The advantage of course is that you can build your Django application for near-infinite scalability of data, and increased uptime.
