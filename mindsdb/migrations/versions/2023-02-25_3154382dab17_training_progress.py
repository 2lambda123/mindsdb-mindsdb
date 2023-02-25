"""training_progress

Revision ID: 3154382dab17
Revises: ee63d868fa84
Create Date: 2023-02-25 15:12:02.828938

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db



# revision identifiers, used by Alembic.
revision = '3154382dab17'
down_revision = 'ee63d868fa84'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('training_phase_current', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('training_phase_total', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('training_phase_name', sa.String(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_column('training_phase_name')
        batch_op.drop_column('training_phase_total')
        batch_op.drop_column('training_phase_current')

    # ### end Alembic commands ###
