import firebase_admin
from firebase_admin import credentials, firestore
import os

def update_user_busy_dates():
    # 1. Initialize the Firebase Admin SDK
    firebase_credentials_path = os.path.join(
        os.getcwd(), 
        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
    )
    cred = credentials.Certificate(firebase_credentials_path)
    firebase_admin.initialize_app(cred)

    db = firestore.client()
    
    # 2. Reference the users and plans collections
    users_collection = db.collection('users')
    plans_collection = db.collection('plans')

    # 3. Set up pagination for the users collection
    page_size = 500  # Adjust as needed
    last_doc = None
    total_users_processed = 0
    total_busy_dates_added = 0

    while True:
        # Build the query for the next batch of users
        query = users_collection.order_by('__name__').limit(page_size)
        if last_doc:
            query = query.start_after({'__name__': last_doc.id})

        user_docs = list(query.stream())
        if not user_docs:
            break  # No more user documents

        for user_doc in user_docs:
            total_users_processed += 1
            user_uid = user_doc.id  # Assuming document ID is the user's UID

            # 4. Query plans for which the user is a team member.
            plans_query = plans_collection.where('teamMembers', 'array_contains', user_uid)
            plans = list(plans_query.stream())

            # Use a batch for writing busyDates for this user.
            batch = db.batch()
            busy_dates_ref = user_doc.reference.collection('busyDates')
            busy_dates_added = 0

            for plan in plans:
                plan_data = plan.to_dict()
                # Ensure the plan has the necessary fields
                plan_id = plan_data.get('planId')
                from_date = plan_data.get('fromDate')
                to_date = plan_data.get('toDate')

                if plan_id and from_date and to_date:
                    # Use planId as the document ID in the busyDates subcollection.
                    busy_doc_ref = busy_dates_ref.document(plan_id)
                    busy_data = {
                        'planId': plan_id,
                        'fromDate': from_date,  # Firestore Timestamp
                        'toDate': to_date       # Firestore Timestamp
                    }
                    batch.set(busy_doc_ref, busy_data)
                    busy_dates_added += 1

            # Commit the batch if there are any busy date entries for the user.
            if busy_dates_added > 0:
                batch.commit()
                total_busy_dates_added += busy_dates_added
                print(f"User {user_uid}: Added {busy_dates_added} busyDates.")
            else:
                print(f"User {user_uid}: No plans found.")

        # Update last_doc for pagination
        last_doc = user_docs[-1]

    print(f"Migration complete. Processed {total_users_processed} users and added {total_busy_dates_added} busyDates.")

if __name__ == '__main__':
    update_user_busy_dates()
