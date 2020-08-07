from . import TestCase
from .models import TestUser


class DjangoInteractionTests(TestCase):

    def test_update_or_create_works(self):
        """
            update_or_create uses Django's atomic()
        """

        user, created = TestUser.objects.update_or_create(
            username="test",
            defaults={
                "email": "test@example.com"
            }
        )

        user_id = user.pk

        self.assertTrue(created)
        self.assertTrue(user_id)

        user, created = TestUser.objects.update_or_create(
            username="test",
            defaults={
                "email": "test2@example.com"
            }
        )

        self.assertFalse(created)
        self.assertEqual(user.pk, user_id)
