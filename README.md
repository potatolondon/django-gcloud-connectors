# Django GCloud Connectors (DGC)

**Note: This project is now living in GitLab: https://gitlab.com/potato-oss/google-cloud/django-gcloud-connectors**

**WARNING: This is very much a work in progress, is unstable, and not ready for use**

The aim of this project is to create Django database connector / backend for Google Cloud.

Currently it contains a connector for the Google Cloud Datastore (Datastore in Firestore mode)
but in the future it may also include a Firestore connector, or even a MemoryStore one.

This is the continuation of the Datastore connector from the [Djangae project](https://github.com/potatolondon/djangae)
but converted to use the [Cloud Datastore API](https://googleapis.github.io/google-cloud-python/latest/datastore/) on Python 3.

If you are interested in submitting a patch, please refer to `CONTRIBUTING.md`


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
start and stop a Cloud Datastore Emulator instance. To enable this functionality
just add `gcloudc.commands` to your `INSTALLED_APPS` setting.

# Release process

Release to pypi is managed by GitLab CI. To create a new release create the relevant tag
and push it to the gitlab remote e.g.

```
git tag 1.0.0
git push origin 1.0.0
```

This will trigger a pipeline that will publish the package in test.pypi.org.
If that is successful, you can then manually trigger the job `publish to prod pypi` on the same pipeline to deploy to the official pypi registry.
