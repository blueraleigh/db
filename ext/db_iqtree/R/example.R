library(db)
library(db.cli)

# STEP 0
#
# initial setup
db = db.open("campanulids.db")
db.sqlar(db, "nt_sequences", "nt_sequences")

db.eval(db, "CREATE TABLE dbpkg(pkg TEXT)")
db.eval(db, "INSERT INTO dbpkg VALUES('db.cli')")

register_cli_module(db)

db.eval(db, "CREATE VIRTUAL TABLE iqtree USING cli(exe='/Applications/iqtree-2.1.2-MacOSX/bin/iqtree2')")
db.eval(db, "CREATE VIRTUAL TABLE mptp USING cli(exe='/Applications/mptp/bin/mptp')")
db.eval(db, "CREATE VIRTUAL TABLE macse USING cli(exe='java -jar /Applications/macse/macse_v2.05.jar')")


# STEP 1
#
# infer alignments for all loci. macse doesn't accept directory
# input so we have to loop over individual fastas. this means 
# that each alignment will live in its own directory under macse_output.
db.lapply(db, 'SELECT name FROM nt_sequences WHERE sz > 0', 
    FUN=function(l) {
        db.eval(db, sprintf("
            SELECT * FROM macse 
            WHERE 
                input='nt_sequences' 
            AND
                output='macse_output' 
            AND
                cmd='-prog alignSequences -seq %s'
        ", l$name))
    }
)


# STEP 2
#
# move macse_ouput files under a single directory in nt_alignments.
# alse replace macse special characters ! and * with standard
# gap character - (otherwise iqtree will choke)

db.sqlar_skeleton(db, "nt_alignments")

db.eval(db, "
    INSERT INTO 
    nt_alignments(name,mode,mtime,sz) 
    VALUES('nt_alignments',?,?,0)", 
    db.eval(db, "SELECT mode,mtime FROM macse_output WHERE name = 'macse_output'")[1,])
db.eval(db, "
    INSERT INTO 
    nt_alignments(name,mode,mtime,sz) 
    VALUES('nt_alignments/alignNT',?,?,0)", 
    db.eval(db, "SELECT mode,mtime FROM macse_output WHERE name = 'macse_output'")[1,])
db.eval(db, "
    INSERT INTO 
    nt_alignments(name,mode,mtime,sz) 
    VALUES('nt_alignments/alignAA',?,?,0)", 
    db.eval(db, "SELECT mode,mtime FROM macse_output WHERE name = 'macse_output'")[1,])

db.lapply(db, "
    SELECT name,mode,mtime,sz,sqlar_uncompress(data,sz) as data 
    FROM macse_output WHERE name LIKE '%NT.fasta'",
    FUN=function(l) {
        re = "(?:(?<=/))([a-zA-Z0-9\\._~-]+)(?=(?:[/?]|$))"
        l$name = sub("macse_output", "nt_alignments", l$name)
        l$name = sub(re, "alignNT", l$name, perl=TRUE)
        l$data = gsub("[!*]", "-", rawToChar(l$data))
        db.eval(db, "INSERT INTO nt_alignments VALUES(?,?,?,?,sqlar_compress(?))", l)
    }
)
db.lapply(db, "
    SELECT name,mode,mtime,sz,sqlar_uncompress(data,sz) as data 
    FROM macse_output WHERE name LIKE '%AA.fasta'",
    FUN=function(l) {
        re = "(?:(?<=/))([a-zA-Z0-9\\._~-]+)(?=(?:[/?]|$))"
        l$name = sub("macse_output", "nt_alignments", l$name)
        l$name = sub(re, "alignAA", l$name, perl=TRUE)
        l$data = gsub("[!*]", "-", rawToChar(l$data))
        db.eval(db, "INSERT INTO nt_alignments VALUES(?,?,?,?,sqlar_compress(?))", l)
    }
)

# STEP 3
#
# run iqtree on nt alignments and then send the 
# iqtree "species tree" to mPTP for "species
# delimitation"
db.exec(db, "
-- infer ML tree from all loci
SELECT * FROM iqtree
WHERE 
input='nt_alignments' 
AND output='iqtree_output'
AND cmd='-p alignNT -m JC --prefix concat -T 1';

-- infer trees for individual loci
SELECT * FROM iqtree
WHERE 
input='nt_alignments' 
AND output='iqtree_output'
AND cmd='-S alignNT -m JC --prefix loci -T 1';

-- run concordance factor analysis
SELECT * FROM iqtree
WHERE 
input='nt_alignments,iqtree_output' 
AND output='iqtree_output'
AND cmd='-t concat.treefile --gcf loci.treefile -p alignNT --scf 100 --prefix concord -T 1';

-- infer species tree
SELECT * FROM mptp
WHERE 
input='iqtree_output' 
AND output='mptp_output'
AND cmd='--ml --multi --tree_file concat.treefile --output_file mptp_spptree';
")


db.unsqlar(db, "nt_sequences", "~/Desktop/campanulids")
db.unsqlar(db, "nt_alignments", "~/Desktop/campanulids")
db.unsqlar(db, "iqtree_output", "~/Desktop/campanulids")
db.unsqlar(db, "mptp_output", "~/Desktop/campanulids")